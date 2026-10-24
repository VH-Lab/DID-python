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
import shutil
import sqlite3

import pytest

from did.document import Document
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


def _move_file_under_handle(path):
    """Replace the database with a byte-identical copy at the same path.

    The open connection still holds the old inode, so its next write fails
    with SQLITE_READONLY_DBMOVED ("attempt to write a readonly database") --
    the poisoned handle da83fa3 reopens to clear. No mocking: this is the
    real SQLite failure.
    """
    shutil.copyfile(path, path + ".moved")
    os.replace(path + ".moved", path)


def _doc(idhex, value=1):
    return Document(
        {
            "base": {"id": idhex, "datestamp": "2020-01-01"},
            "document_class": {"class_name": "thing"},
            "demoA": {"value": value},
        }
    )


def _doc_ids_on_disk(path):
    with sqlite3.connect(path) as check:
        return {row[0] for row in check.execute("SELECT doc_id FROM docs")}


class TestPoisonedHandleRecovery:
    """A READONLY inside a write operation: reopen, replay, carry on."""

    def test_the_failure_is_real(self, db):
        db.add_branch("main")
        _move_file_under_handle(db.connection)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.dbid.execute("INSERT INTO branches VALUES ('x', NULL, 0)")

    def test_add_doc_is_recovered_after_the_file_moved(self, db, sleeps):
        db.add_branch("main")
        poisoned = db.dbid
        _move_file_under_handle(db.connection)

        db.add_docs([_doc("a" * 32)], branch_id="main", validate=False)

        assert sleeps == [0.1]
        assert db.dbid is not poisoned
        assert "a" * 32 in _doc_ids_on_disk(db.connection)
        got = db.get_docs("a" * 32, branch_id="main")
        assert got.document_properties["demoA"]["value"] == 1

    def test_add_branch_is_recovered(self, db, sleeps):
        db.add_branch("main")
        _move_file_under_handle(db.connection)

        db.add_branch("b2", parent_branch_id="main")

        assert sleeps == [0.1]
        assert {"main", "b2"} <= _branch_ids(db.connection)

    def test_remove_doc_is_recovered(self, db, sleeps):
        db.add_branch("main")
        db.add_docs([_doc("b" * 32)], branch_id="main", validate=False)
        _move_file_under_handle(db.connection)

        db.remove_docs(["b" * 32], branch_id="main")

        assert sleeps == [0.1]
        assert "b" * 32 not in _doc_ids_on_disk(db.connection)

    def test_the_journal_ends_with_the_operation(self, db, sleeps):
        db.add_branch("main")
        _move_file_under_handle(db.connection)
        db.add_docs([_doc("c" * 32)], branch_id="main", validate=False)
        assert db._journal is None
        # The next operation replays nothing from the last one.
        _move_file_under_handle(db.connection)
        db.add_docs([_doc("d" * 32, 2)], branch_id="main", validate=False)
        assert {"c" * 32, "d" * 32} <= _doc_ids_on_disk(db.connection)

    def test_a_replay_that_reads_a_different_database_is_refused(
        self, db, sleeps, capsys
    ):
        # The file now at the path is an older copy, without the branch the
        # operation already checked for on the poisoned handle. Committing a
        # replay against it would write a document into a branch that does
        # not exist there, so it is refused and nothing is written.
        snapshot = db.connection + ".before"
        shutil.copyfile(db.connection, snapshot)
        db.add_branch("main")
        os.replace(snapshot, db.connection)

        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.add_docs([_doc("e" * 32)], branch_id="main", validate=False)

        assert sleeps == [0.1]  # diverged: not retried
        assert "e" * 32 not in _doc_ids_on_disk(db.connection)
        assert "read different rows on replay" in capsys.readouterr().err

    def test_without_a_journal_a_readonly_handle_is_not_replayed(self, db, sleeps):
        # Outside a write operation, do_run_sql_query's own retry applies.
        assert db._journal is None
        db.add_branch("main")
        _move_file_under_handle(db.connection)
        db.do_run_sql_query(INSERT_BRANCH, ("x", None, 0.0))
        db._commit()
        assert "x" in _branch_ids(db.connection)


class TestRecoveryThatCannotReopen:
    def test_the_original_error_reaches_the_caller(self, db, sleeps, monkeypatch):
        db.add_branch("main")
        db.dbid = _ReadonlyAtCommit(db.dbid)
        real_open = SQLiteDB._open_db

        def refuse(self):
            raise sqlite3.OperationalError("unable to open database file")

        monkeypatch.setattr(SQLiteDB, "_open_db", refuse)
        with pytest.raises(sqlite3.OperationalError):
            db.add_docs([_doc("7" * 32)], branch_id="main", validate=False)
        assert len(sleeps) == 6  # every attempt tried to reopen

        monkeypatch.setattr(SQLiteDB, "_open_db", real_open)
        db._open_db()
        assert "7" * 32 not in _doc_ids_on_disk(db.connection)


class _ReadonlyAtCommit:
    """A connection whose next COMMITs fail READONLY, as a poisoned one does.

    SQLite reports DBMOVED at a transaction's first write, so a READONLY that
    reaches COMMIT comes from elsewhere (a hot journal, a remount); it cannot
    be produced on demand, so this one is a stand-in around a real handle.
    """

    def __init__(self, conn):
        self._conn = conn

    def commit(self):
        # Every time: waiting never clears a poisoned handle, only a reopen.
        raise sqlite3.OperationalError("attempt to write a readonly database")

    def __getattr__(self, name):
        return getattr(self._conn, name)


class TestReadonlyCommitRecovery:
    def test_a_readonly_commit_is_replayed_on_a_fresh_connection(self, db, sleeps):
        db.add_branch("main")
        db.dbid = _ReadonlyAtCommit(db.dbid)

        db.add_docs([_doc("f" * 32)], branch_id="main", validate=False)

        assert sleeps == [0.1]
        assert isinstance(db.dbid, sqlite3.Connection)  # reopened
        assert "f" * 32 in _doc_ids_on_disk(db.connection)

    def test_files_ingested_before_the_commit_are_not_needed_again(self, db, sleeps):
        # The ingest copies the file into FileDir and, with delete_original,
        # deletes the source -- all before COMMIT. Running the operation
        # again would fail on the missing source; replaying its SQL does not
        # need the file at all.
        db.add_branch("main")
        source = os.path.join(os.path.dirname(db.connection), "source.bin")
        with open(source, "wb") as handle:
            handle.write(b"payload")
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", "placeholder", ingest=0, delete_original=0)
        doc.add_file("filename2.ext", "placeholder", ingest=0, delete_original=0)
        _, info, _ = doc.is_in_file_list("filename1.ext")
        info["locations"] = [
            {
                "location": source,
                "location_type": "file",
                "uid": "u-local",
                "ingest": 1,
                "delete_original": 1,
                "parameters": "",
            }
        ]
        db.dbid = _ReadonlyAtCommit(db.dbid)

        db.add_docs([doc], branch_id="main", validate=False)

        assert sleeps == [0.1]
        assert not os.path.exists(source)
        file_obj = db.open_doc(doc.id(), "filename1.ext")
        file_obj.fopen()
        try:
            assert file_obj.fread() == b"payload"
        finally:
            file_obj.fclose()

    def test_a_busy_commit_is_still_waited_out_not_replayed(self, db, sleeps):
        db.add_branch("main")
        reader = sqlite3.connect(db.connection, isolation_level=None)
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM branches").fetchall()
        sleeps.on_sleep.append(reader.rollback)
        handle = db.dbid

        db.add_docs([_doc("1" * 32)], branch_id="main", validate=False)

        assert db.dbid is handle
        assert "1" * 32 in _doc_ids_on_disk(db.connection)
        reader.close()


class TestReplayRefusesADifferentDatabase:
    """A replay that does not do what the original did is never committed."""

    def test_another_writer_in_between_is_refused(self, db, sleeps, capsys):
        # While we wait to reopen, another connection adds a document. Our
        # replayed INSERT then gets a different doc_idx, so every doc_data
        # row built from the first one would point at the wrong document.
        db.add_branch("main")
        db.dbid = _ReadonlyAtCommit(db.dbid)
        other = SQLiteDB(db.connection)
        sleeps.on_sleep.append(
            lambda: other.add_docs([_doc("8" * 32)], "main", validate=False)
        )

        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.add_docs([_doc("9" * 32)], branch_id="main", validate=False)

        on_disk = _doc_ids_on_disk(db.connection)
        assert "8" * 32 in on_disk
        assert "9" * 32 not in on_disk
        assert sleeps == [0.1]  # a divergence is not retried
        assert "inserted row" in capsys.readouterr().err
        other._close_db()

    def test_a_statement_that_fails_on_replay_is_divergence(self, db, sleeps):
        # Replaying a journal that is already on disk: its first INSERT now
        # hits a UNIQUE row. That is reported as divergence, not as the
        # IntegrityError, so the caller still sees the original failure.
        db.add_branch("main")
        journal = []
        original = db._journal_committed

        def capture():
            journal.extend(dict(entry) for entry in db._journal)
            original()

        db._journal_committed = capture
        db.add_branch("b2", parent_branch_id="main")
        db._journal_committed = original

        with pytest.raises(sqlitedb_module._ReplayDiverged, match="failed on replay"):
            db._replay(journal)
        db.dbid.rollback()


class TestFileReplacedAfterTheFirstWrite:
    """The swap SQLite does not see: after the transaction's first write.

    SQLite's DBMOVED check runs only when a write transaction starts. A
    file replaced later is not noticed, and COMMIT succeeds into the old,
    now nameless file -- the write is lost with no error. SQLiteDB checks
    that the path still names its file just before COMMIT.
    """

    def _swap_during_add(self, monkeypatch, action):
        # _populate_files runs after the docs and doc_data INSERTs, so the
        # write transaction is already open when the file goes.
        original = SQLiteDB._populate_files

        def populate_then_swap(self, *args, **kwargs):
            action(self.connection)
            return original(self, *args, **kwargs)

        monkeypatch.setattr(SQLiteDB, "_populate_files", populate_then_swap)

    def test_sqlite_alone_loses_the_write(self, tmp_path):
        # The failure, shown on a bare connection: no error, no data.
        path = str(tmp_path / "bare.sqlite")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (a)")
        conn.commit()
        conn.execute("INSERT INTO t VALUES (1)")
        _move_file_under_handle(path)
        conn.commit()  # succeeds
        conn.close()
        with sqlite3.connect(path) as check:
            assert check.execute("SELECT * FROM t").fetchall() == []

    def test_a_replaced_file_is_caught_and_the_write_lands(
        self, db, sleeps, monkeypatch
    ):
        db.add_branch("main")
        self._swap_during_add(monkeypatch, _move_file_under_handle)

        db.add_docs([_doc("5" * 32)], branch_id="main", validate=False)

        assert sleeps == [0.1]
        assert "5" * 32 in _doc_ids_on_disk(db.connection)

    def test_a_deleted_file_is_an_error_not_a_silent_loss(
        self, db, sleeps, monkeypatch
    ):
        db.add_branch("main")
        self._swap_during_add(monkeypatch, os.remove)

        with pytest.raises(sqlite3.OperationalError, match="was replaced") as caught:
            db.add_docs([_doc("6" * 32)], branch_id="main", validate=False)
        assert isinstance(caught.value.__cause__, FileNotFoundError)
        # Not reopened: that would have created a new, empty database at the
        # path and replayed into it.
        assert not os.path.exists(db.connection)

    def test_a_false_alarm_costs_one_replay_onto_the_same_file(
        self, db, sleeps, monkeypatch
    ):
        # A volume with unstable inode numbers: the path seems to have moved
        # once, but it is the same file. Nothing was committed before the
        # check, so the replay lands the document exactly once.
        db.add_branch("main")
        real = SQLiteDB._file_identity
        calls = {"n": 0}

        def flaky(self):
            calls["n"] += 1
            if calls["n"] == 1:
                return ("elsewhere", 0)
            return real(self)

        monkeypatch.setattr(SQLiteDB, "_file_identity", flaky)

        db.add_docs([_doc("4" * 32)], branch_id="main", validate=False)

        assert sleeps == [0.1]
        with sqlite3.connect(db.connection) as check:
            count = check.execute(
                "SELECT COUNT(*) FROM docs WHERE doc_id = ?", ("4" * 32,)
            ).fetchone()[0]
        assert count == 1

    def test_an_unchanged_file_is_not_replayed(self, db, sleeps):
        db.add_branch("main")
        handle = db.dbid
        db.add_docs([_doc("3" * 32)], branch_id="main", validate=False)
        assert sleeps == []
        assert db.dbid is handle

    def test_the_identity_follows_a_reopen(self, db, sleeps):
        db.add_branch("main")
        _move_file_under_handle(db.connection)
        db.add_branch("b2", parent_branch_id="main")  # recovered by reopen
        assert db._db_identity == db._file_identity()
        assert not db._file_was_replaced()


class TestOriginalsAreDeletedAfterCommit:
    """delete_original waits for the commit, so a rollback keeps the file."""

    def _file_doc(self, db):
        source = os.path.join(os.path.dirname(db.connection), "source.bin")
        with open(source, "wb") as handle:
            handle.write(b"payload")
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", "placeholder", ingest=0, delete_original=0)
        doc.add_file("filename2.ext", "placeholder", ingest=0, delete_original=0)
        _, info, _ = doc.is_in_file_list("filename1.ext")
        info["locations"] = [
            {
                "location": source,
                "location_type": "file",
                "uid": "u-local",
                "ingest": 1,
                "delete_original": 1,
                "parameters": "",
            }
        ]
        return doc, source

    def test_a_rolled_back_add_keeps_the_original(self, db, monkeypatch):
        db.add_branch("main")
        doc, source = self._file_doc(db)

        def refuse(self):
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr(SQLiteDB, "_commit", refuse)
        with pytest.raises(sqlite3.OperationalError, match="disk I/O"):
            db.add_docs([doc], branch_id="main", validate=False)
        assert os.path.exists(source), "a rollback must not delete the original"
        assert doc.id() not in _doc_ids_on_disk(db.connection)

        monkeypatch.undo()
        db.add_docs([doc], branch_id="main", validate=False)
        assert not os.path.exists(source), "deleted once the add commits"
        assert doc.id() in _doc_ids_on_disk(db.connection)

    def test_nothing_is_left_queued_after_an_operation(self, db):
        db.add_branch("main")
        doc, _ = self._file_doc(db)
        db.add_docs([doc], branch_id="main", validate=False)
        assert db._pending_deletes is None
