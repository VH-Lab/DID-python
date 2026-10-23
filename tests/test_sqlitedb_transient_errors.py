"""Retry, reopen and diagnostics for transient SQLite write failures.

Ported from DID-matlab decd155 and da83fa3 (sqlitedb.m). On a slow, networked
or removable drive a write can fail with SQLITE_BUSY ("database is locked") or
SQLITE_READONLY ("attempt to write a readonly database") and succeed moments
later. DID-matlab now:

* sets ``busy_timeout=30000`` on open, so a lock is waited for;
* retries a BUSY / READONLY statement with backoff (0.1 .. 4 s);
* reopens the connection before retrying a READONLY, which a poisoned handle
  (SQLITE_READONLY_DBMOVED) needs and waiting never cures;
* prints filesystem diagnostics when the retries are exhausted.

The failures here are real sqlite3 errors, not mocks: a second connection
holds the lock, or the handle is swapped for a read-only one. Only
``time.sleep`` is replaced, so a test never waits out the backoff -- and the
fake sleep is the hook that releases the lock "while we waited".
"""

import os
import sqlite3

import pytest

from did.implementations import sqlitedb as sqlitedb_module
from did.implementations.sqlitedb import SQLiteDB

INSERT_BRANCH = (
    "INSERT INTO branches (branch_id, parent_id, timestamp) VALUES (?, ?, ?)"
)


@pytest.fixture
def db(tmp_path):
    database = SQLiteDB(str(tmp_path / "transient.sqlite"))
    # Fail at once on a lock instead of waiting 30 s, so the retry loop -- not
    # SQLite's busy handler -- is what each test exercises.
    database.dbid.execute("PRAGMA busy_timeout = 0")
    yield database
    database._close_db()


class _Sleeps(list):
    """The backoffs requested, plus side effects to run on the next sleeps."""

    def __init__(self):
        super().__init__()
        self.on_sleep = []

    def __call__(self, seconds):
        self.append(seconds)
        if self.on_sleep:
            self.on_sleep.pop(0)()


@pytest.fixture
def sleeps(monkeypatch):
    """Record each backoff instead of sleeping."""
    fake = _Sleeps()
    monkeypatch.setattr(sqlitedb_module.time, "sleep", fake)
    return fake


def _hold_write_lock(path):
    """A second connection holding the RESERVED lock until rolled back."""
    other = sqlite3.connect(path, isolation_level=None)
    other.execute("BEGIN IMMEDIATE")
    return other


def _branch_ids(path):
    with sqlite3.connect(path) as check:
        return {row[0] for row in check.execute("SELECT branch_id FROM branches")}


class TestBusyTimeout:
    def test_open_sets_a_30_second_busy_timeout(self, tmp_path):
        database = SQLiteDB(str(tmp_path / "timeout.sqlite"))
        try:
            timeout = database.dbid.execute("PRAGMA busy_timeout").fetchone()[0]
            assert timeout == 30000
        finally:
            database._close_db()


class TestErrorClassification:
    @pytest.mark.parametrize(
        "message",
        [
            "attempt to write a readonly database",
            "database is locked",
            "database table is locked",
            "Database is BUSY",
        ],
    )
    def test_transient_sqlite_errors_are_retryable(self, message):
        assert SQLiteDB._is_retryable_sql_error(sqlite3.OperationalError(message))

    @pytest.mark.parametrize(
        "message", ['near "SELEC": syntax error', "no such table: nope"]
    )
    def test_other_sqlite_errors_are_not(self, message):
        assert not SQLiteDB._is_retryable_sql_error(sqlite3.OperationalError(message))

    def test_only_sqlite_errors_count(self):
        assert not SQLiteDB._is_retryable_sql_error(ValueError("read only"))
        assert not SQLiteDB._is_readonly_error(OSError("read-only file system"))

    def test_readonly_is_distinguished_from_busy(self):
        assert SQLiteDB._is_readonly_error(
            sqlite3.OperationalError("attempt to write a readonly database")
        )
        assert not SQLiteDB._is_readonly_error(
            sqlite3.OperationalError("database is locked")
        )


class TestStatementRetry:
    def test_busy_write_is_retried_until_the_lock_is_released(self, db, sleeps):
        other = _hold_write_lock(db.connection)
        sleeps.on_sleep.append(other.rollback)
        handle = db.dbid

        db.do_run_sql_query(INSERT_BRANCH, ("b1", None, 0.0))
        db._commit()

        assert sleeps == [0.1]
        assert db.dbid is handle  # BUSY is waited out, never reopened
        assert "b1" in _branch_ids(db.connection)
        other.close()

    def test_readonly_handle_is_reopened_before_the_retry(self, db, sleeps):
        # A handle that refuses writes on a file that is perfectly writable --
        # what SQLITE_READONLY_DBMOVED looks like from the caller's side.
        db.dbid.close()
        db.dbid = sqlite3.connect(f"file:{db.connection}?mode=ro", uri=True)
        db.dbid.row_factory = sqlite3.Row
        poisoned = db.dbid

        db.do_run_sql_query(INSERT_BRANCH, ("b1", None, 0.0))
        db._commit()

        assert sleeps == [0.1]
        assert db.dbid is not poisoned
        assert "b1" in _branch_ids(db.connection)

    def test_reopened_connection_keeps_the_busy_timeout(self, db, sleeps):
        db.dbid.close()
        db.dbid = sqlite3.connect(f"file:{db.connection}?mode=ro", uri=True)
        db.do_run_sql_query(INSERT_BRANCH, ("b1", None, 0.0))
        assert db.dbid.execute("PRAGMA busy_timeout").fetchone()[0] == 30000

    def test_retries_are_exhausted_then_the_error_is_raised(self, db, sleeps, capsys):
        other = _hold_write_lock(db.connection)
        try:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                db.do_run_sql_query(INSERT_BRANCH, ("b1", None, 0.0))
        finally:
            other.rollback()
            other.close()

        assert sleeps == [0.1, 0.2, 0.5, 1, 2, 4]
        err = capsys.readouterr().err
        assert "SQLite readonly/busy diagnostics:" in err
        assert f"db file: {db.connection}" in err

    def test_a_non_transient_error_is_not_retried(self, db, sleeps, capsys):
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            db.do_run_sql_query("SELECT * FROM no_such_table")
        assert sleeps == []
        assert "diagnostics" not in capsys.readouterr().err

    def test_no_retry_inside_an_open_transaction(self, db, sleeps, capsys):
        # Rolling back or reopening to retry would silently drop work already
        # pending in this transaction, so the error goes straight to the
        # caller, who rolls back the whole unit.
        db.dbid.execute("BEGIN")
        db.do_run_sql_query("SELECT * FROM branches")
        handle = db.dbid
        other = _hold_write_lock(db.connection)
        try:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                db.do_run_sql_query(INSERT_BRANCH, ("b1", None, 0.0))
        finally:
            other.rollback()
            other.close()

        assert sleeps == []
        assert db.dbid is handle
        assert db.dbid.in_transaction  # left for the caller to roll back
        # Not retried, but still reported.
        assert "SQLite readonly/busy diagnostics:" in capsys.readouterr().err
        db.dbid.rollback()


class TestCommitRetry:
    def test_busy_commit_is_retried_and_the_transaction_lands(self, db, sleeps):
        # In Python the write reaches the disk at COMMIT, so that is where a
        # slow drive's transient failure shows up.
        db.do_run_sql_query(INSERT_BRANCH, ("b1", None, 0.0))
        reader = sqlite3.connect(db.connection, isolation_level=None)
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM branches").fetchall()  # SHARED lock
        sleeps.on_sleep.append(reader.rollback)
        handle = db.dbid

        db._commit()

        assert sleeps == [0.1]
        assert db.dbid is handle  # never reopened: that would lose the write
        assert "b1" in _branch_ids(db.connection)
        reader.close()

    def test_exhausted_commit_retries_raise_with_diagnostics(self, db, sleeps, capsys):
        db.do_run_sql_query(INSERT_BRANCH, ("b1", None, 0.0))
        reader = sqlite3.connect(db.connection, isolation_level=None)
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM branches").fetchall()
        try:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                db._commit()
        finally:
            reader.rollback()
            reader.close()
        assert sleeps == [0.1, 0.2, 0.5, 1, 2, 4]
        assert "SQLite readonly/busy diagnostics:" in capsys.readouterr().err
        db.dbid.rollback()

    def test_add_branch_commits_through_the_retry(self, db, sleeps):
        reader = sqlite3.connect(db.connection, isolation_level=None)
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM branches").fetchall()
        sleeps.on_sleep.append(reader.rollback)

        db.add_branch("b1")

        assert sleeps == [0.1]
        assert "b1" in _branch_ids(db.connection)
        reader.close()


class TestReadonlyDiagnostics:
    def test_reports_the_file_and_a_writable_directory(self, db):
        diag = db._collect_readonly_diagnostics()
        assert f"db file: {db.connection}" in diag
        assert "db file UserWrite=1" in diag
        assert "db dir writable: yes" in diag

    def test_the_directory_probe_leaves_nothing_behind(self, db):
        before = set(os.listdir(os.path.dirname(db.connection)))
        db._collect_readonly_diagnostics()
        assert set(os.listdir(os.path.dirname(db.connection))) == before

    def test_reports_a_hot_journal_beside_the_database(self, db):
        with open(db.connection + "-journal", "wb") as journal:
            journal.write(b"x" * 7)
        try:
            diag = db._collect_readonly_diagnostics()
        finally:
            os.remove(db.connection + "-journal")
        assert "sibling -journal present (7 bytes)" in diag

    def test_reports_an_unwritable_directory(self, db, monkeypatch):
        def refuse(*args, **kwargs):
            raise PermissionError("Read-only file system")

        monkeypatch.setattr(sqlitedb_module.tempfile, "NamedTemporaryFile", refuse)
        diag = db._collect_readonly_diagnostics()
        assert "db dir writable: NO (Read-only file system)" in diag
