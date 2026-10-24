import contextlib
import functools
import os
import re as _re
import shutil
import sqlite3
import struct
import sys
import tempfile
import time
import warnings
from datetime import datetime, timezone

from ..database import Database
from ..file import is_safe_uid


def _sqlite_regexp(pattern, string):
    """SQLite regexp function implementation."""
    if string is None:
        return None
    try:
        return 1 if _re.search(pattern, str(string)) else None
    except _re.error:
        return None


def _sql_escape(value):
    """Escape single quotes for SQL string literals."""
    if value is None:
        return ""
    return str(value).replace("'", "''")


# How long a statement waits for another connection's lock before SQLite gives
# up with SQLITE_BUSY ("database is locked"). DID-matlab's open_db sets
# ``pragma busy_timeout=30000``; sqlite3.connect's ``timeout`` is the same
# setting, in seconds.
_BUSY_TIMEOUT = 30.0

# Seconds to wait between attempts at a transient BUSY / READONLY failure --
# the schedule DID-matlab's run_sql_noOpen uses.
_RETRY_BACKOFFS = (0.1, 0.2, 0.5, 1, 2, 4)


class _ReplayDiverged(Exception):
    """A replayed transaction saw a different database than the original."""


class _JournalCursor:
    """A cursor that records what each of its statements did.

    Used inside a write operation (see :func:`_journaled`) so that, when the
    connection is poisoned part-way through, the transaction can be replayed
    on a fresh connection and checked against what it saw the first time --
    see :meth:`SQLiteDB._recover_transaction`. Each statement's entry holds
    its SQL and parameters, its rowcount, the lastrowid of an INSERT, and
    every row the caller fetched.
    """

    def __init__(self, db, cursor):
        self._db = db
        self._cursor = cursor
        self._entry = None

    def execute(self, sql, params=()):
        return self._db._journaled_execute(self, "execute", sql, params)

    def executemany(self, sql, seq_of_params):
        # Materialized so the same parameters can be replayed.
        return self._db._journaled_execute(
            self, "executemany", sql, list(seq_of_params)
        )

    def _seen(self, rows, complete):
        if self._entry is not None:
            self._entry["rows"].extend(tuple(row) for row in rows)
            if complete:
                self._entry["complete"] = True

    def fetchone(self):
        row = self._cursor.fetchone()
        self._seen([] if row is None else [row], row is None)
        return row

    def fetchall(self):
        rows = self._cursor.fetchall()
        self._seen(rows, True)
        return rows

    def fetchmany(self, size=None):
        rows = self._cursor.fetchmany(self._cursor.arraysize if size is None else size)
        self._seen(rows, not rows)
        return rows

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def close(self):
        self._cursor.close()


def _journaled(method):
    """Record the SQL a write operation runs, so it can be recovered.

    Re-entrant: an operation called from inside another shares its journal.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self._journal is not None:
            return method(self, *args, **kwargs)
        self._journal = []
        self._pending_deletes = []
        try:
            return method(self, *args, **kwargs)
        finally:
            # Whatever was not committed keeps its files (see
            # _delete_after_commit).
            self._journal = None
            self._pending_deletes = None

    return wrapper


def _is_insert(sql):
    return sql.lstrip().upper().startswith(("INSERT", "REPLACE"))


# Escape character used in LIKE patterns (see _sql_like_escape / ESCAPE clauses).
_LIKE_ESCAPE_CHAR = "\\"


def _sql_like_escape(value):
    """Escape LIKE wildcards in a literal operand of a LIKE pattern.

    '%' and '_' are LIKE wildcards; without escaping, a field name containing
    '_' would match any single character (e.g. 'a_b' would also match 'axb'),
    producing false-positive matches. Callers that embed the result inside a
    LIKE pattern must also append "ESCAPE '\\'" so the backslash is treated as
    the escape character. The single-quote escaping for the surrounding SQL
    string literal is applied on top of this by _sql_escape.
    """
    if value is None:
        return ""
    text = str(value)
    text = text.replace(_LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
    text = text.replace("%", _LIKE_ESCAPE_CHAR + "%")
    text = text.replace("_", _LIKE_ESCAPE_CHAR + "_")
    return text


# Most document ids one search query lists when it checks a search term
# against candidate documents (see SQLiteDB._search_doc_ids); also the most
# ORed base.id values looked up in one query. DID-matlab's
# did.database.maxSearchCandidates.
_MAX_SEARCH_CANDIDATES = 1000

# The doc_data indexes searches rely on -- see SQLiteDB._ensure_indexes. The
# names are the same in DID-matlab (sqlitedb.searchIndexes).
_SEARCH_INDEXES = (
    ("doc_data_value", "value"),
    ("doc_data_field_value", "field_idx, value"),
    ("doc_data_doc_idx", "doc_idx"),
)

# Operations that test every value they see -- in SQL or by loading every
# document (brute force) -- and so are searched last in an AND, against the
# documents the other terms left. DID-matlab ranks regexp, contains_string
# and hasmember this way; the rest are the operations only Python sends to
# brute force.
_SCAN_OPERATIONS = frozenset(
    {
        "regexp",
        "contains_string",
        "hasmember",
        "hasanysubfield_exact_string",
        "hasanysubfield_contains_string",
        "hassize",
        "partial_struct",
    }
)


def _sql_string_list(values):
    """A comma-separated list of single-quoted SQL string literals."""
    return ",".join(f"'{_sql_escape(v)}'" for v in values)


def _base_id_lookup_values(search_struct):
    """Is a search structure a lookup of documents by base.id?

    Returns ``(True, ids)`` if *search_struct* is a ``base.id`` exact_string
    match for a non-empty id, or an OR of such matches nested to any depth;
    otherwise ``(False, [])``. Negated operations are not lookups. An OR's
    operands are lists; a one-element list is its element, a longer one is an
    AND and not a lookup. The tree is walked with a stack, not recursion, so a
    query built as ``q = q | Query("base.id", ...)`` in a loop cannot run into
    the recursion limit. Mirrors DID-matlab's did.database.baseIdLookupValues.
    """
    found = []
    pending = [search_struct]
    while pending:
        item = pending.pop()
        if isinstance(item, list):
            if len(item) != 1:
                return False, []
            item = item[0]
        if not isinstance(item, dict):
            return False, []
        op = str(item.get("operation") or "").strip().lower()
        if op == "exact_string":
            value = item.get("param1")
            if (
                item.get("field") != "base.id"
                or not isinstance(value, str)
                or not value
            ):
                return False, []
            found.append(value)
        elif op == "or":
            pending.extend([item.get("param2"), item.get("param1")])
        else:
            return False, []
    return True, found


def _search_rank(item):
    """Where a term of an AND is searched -- see SQLiteDB._search_doc_ids."""
    if _base_id_lookup_values(item)[0]:
        return 1
    if not isinstance(item, dict):
        return 2
    op = str(item.get("operation") or "").lstrip("~").strip().lower()
    if op in _SCAN_OPERATIONS:
        return 4
    if item.get("field") == "base.session_id":
        return 3
    return 2


def _is_safe_uid(uid):
    """Is ``uid`` safe to use as a filename inside FileDir?

    A uid stands in for a file basename under ``<FileDir>/<uid>``, so any
    value that would leave that directory once joined has to be refused.
    Otherwise a document whose ``file_list[i].locations[j].uid`` is
    ``"../../.ssh/authorized_keys"`` reaches ``os.path.join(file_dir, uid)``
    and either writes to, or reads from, an attacker-chosen path outside
    FileDir. See DID-python issue #58.

    Delegates to :func:`did.file.is_safe_uid`, as MATLAB's
    ``sqlitedb.isSafeUid`` delegates to ``did.file.isSafeUid``, so the
    predicate lives in one place.
    """
    return is_safe_uid(uid)


def _is_within(root, path):
    """Does ``path`` resolve inside ``root`` (or equal it)?

    Both are compared with symlinks resolved so a symlink under ``root``
    that points outside cannot smuggle a read past the guard.
    """
    try:
        root_r = os.path.realpath(root)
        path_r = os.path.realpath(path)
    except OSError:
        return False
    try:
        return os.path.commonpath([root_r, path_r]) == root_r
    except ValueError:
        # commonpath raises when the two are on different drives (Windows)
        # or one is empty; either way ``path`` is not contained in ``root``.
        return False


class _SeriesMemberLocations(list):
    """An empty locations list that carries the document + member filename.

    Returned by :meth:`SQLiteDB._locations_for_file` when the requested
    filename is a file series member: series members carry no ``file_info``
    entry and no files-table row (resolution goes through the manifest), so
    the "no locations" answer here is legitimate rather than a miss. The
    tuple stashed on the list lets ``open_doc`` / ``exist_doc`` resolve the
    member without re-fetching the document.
    """

    def __init__(self, document_obj, filename):
        super().__init__()
        self.document_obj = document_obj
        self.filename = filename


class SQLiteDB(Database):
    """A DID database in a SQLite file, with ingested files in ``files/``.

    Keep a database that is in use out of folders a sync client manages
    (Dropbox, OneDrive, iCloud Drive, Google Drive): such clients replace,
    overwrite or fork the file under an open connection. See "Where to keep
    the database file" in docs/sqlitedb.md.
    """

    def __init__(self, filename):
        super().__init__(connection=filename)
        self.dbid = None
        self._fields_cache = {}  # (class, field_name) -> field_idx
        self._journal = None  # statements of the current write operation
        self._pending_deletes = None  # originals to delete once it commits
        self._db_identity = None  # (st_dev, st_ino) of the file opened
        # Do the _SEARCH_INDEXES exist? Set by _ensure_indexes.
        self._search_indexes_ready = False
        self._open_db()

        # Create FileDir now, as DID-MATLAB's sqlitedb constructor does:
        #
        #     cacheDir = fullfile(cacheDir_parent, 'files');
        #     if ~isfolder(cacheDir)
        #         mkdir(cacheDir);
        #     end
        #
        # _file_dir() only computes the path; until this, nothing created the
        # directory, so it appeared on first ingest rather than at database
        # creation. A database that had never stored a file therefore had no
        # files/ at all, and its directory listing differed from the MATLAB
        # equivalent -- which the cross-language symmetry tests compare.
        os.makedirs(self._file_dir(), exist_ok=True)

    def _open_db(self):
        if self.dbid:
            return

        is_new = not os.path.exists(self.connection)
        # Wait (rather than fail at once) while another connection holds a
        # lock: on a slow, networked or removable drive a lock can take a
        # noticeable time to release. Mirrors DID-matlab's busy_timeout=30000.
        self.dbid = sqlite3.connect(self.connection, timeout=_BUSY_TIMEOUT)
        self.dbid.execute("PRAGMA foreign_keys = ON")
        self.dbid.row_factory = sqlite3.Row

        if is_new:
            self._create_db_tables()
        # Which file the connection holds, so a later COMMIT can tell whether
        # the path still names it -- see _file_was_replaced.
        self._db_identity = self._file_identity()
        # Always ensure the search-critical indexes exist — including on
        # databases created before this fix (idempotent).
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create the indexes the document search depends on, if missing.

        The search is a 4-table join over docs/branch_docs/doc_data/fields
        that filters on ``doc_data.field_idx`` + ``doc_data.value`` and joins
        on ``doc_data.doc_idx``. ``doc_data`` (one row per field per doc — by
        far the largest table) had NO index, so every search did a full-scan
        nested-loop join — pathologically slow on large datasets (a single
        ``getprobes`` took ~70 s on a 115 MB cloud dataset).

        This restores the indexing the DID-MATLAB reference already has
        (sqlitedb.m creates ``doc_data(value)``) — the Python port dropped
        it — and adds ``doc_data(field_idx, value)`` (more targeted for the
        actual field+value search) and ``doc_data(doc_idx)`` (the doc join).
        Run on every open (``IF NOT EXISTS``) so databases downloaded before
        this fix benefit on next use. (docs.doc_id and fields.field_name are
        already covered by UNIQUE constraints.)

        DID-matlab creates the same indexes, under the same names
        (``_SEARCH_INDEXES``), and adds them to a database it opens without
        them. ``_search_doc_ids`` relies on two of them -- it names
        ``doc_data_doc_idx`` with ``INDEXED BY`` -- so it uses them only once
        they are known to exist (``_search_indexes_ready``).

        A database that cannot be written to (a read-only file, or one locked
        by another writer) is left as it is, as DID-matlab leaves it: the
        indexes only speed searches up, and without them searches run as they
        did before.
        """
        self._search_indexes_ready = False
        try:
            existing = {
                row[0]
                for row in self.dbid.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index'"
                )
            }
            missing = [idx for idx in _SEARCH_INDEXES if idx[0] not in existing]
            if missing:
                cursor = self.dbid.cursor()
                for name, columns in missing:
                    cursor.execute(
                        f'CREATE INDEX IF NOT EXISTS "{name}" ON doc_data({columns})'
                    )
                self.dbid.commit()
        except sqlite3.Error:
            with contextlib.suppress(sqlite3.Error):
                self.dbid.rollback()
            return
        self._search_indexes_ready = True

    def _close_db(self):
        if self.dbid:
            self.dbid.close()
            self.dbid = None

    def _create_db_tables(self):
        cursor = self.dbid.cursor()

        # Create branches table
        cursor.execute("""
            CREATE TABLE branches (
                branch_id TEXT NOT NULL UNIQUE,
                parent_id TEXT,
                timestamp REAL,
                FOREIGN KEY(parent_id) REFERENCES branches(branch_id),
                PRIMARY KEY(branch_id)
            )
        """)

        # Create docs table
        cursor.execute("""
            CREATE TABLE docs (
                doc_id TEXT NOT NULL UNIQUE,
                doc_idx INTEGER NOT NULL UNIQUE,
                json_code TEXT,
                timestamp REAL,
                PRIMARY KEY(doc_idx AUTOINCREMENT)
            )
        """)

        # Create branch_docs table
        cursor.execute("""
            CREATE TABLE branch_docs (
                branch_id TEXT NOT NULL,
                doc_idx INTEGER NOT NULL,
                timestamp REAL,
                FOREIGN KEY(branch_id) REFERENCES branches(branch_id),
                FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx),
                PRIMARY KEY(branch_id, doc_idx)
            )
        """)

        # Create fields table
        cursor.execute("""
            CREATE TABLE fields (
                class TEXT NOT NULL,
                field_name TEXT NOT NULL UNIQUE,
                json_name TEXT NOT NULL,
                field_idx INTEGER NOT NULL UNIQUE,
                PRIMARY KEY(field_idx AUTOINCREMENT)
            )
        """)

        # Create doc_data table
        cursor.execute("""
            CREATE TABLE doc_data (
                doc_idx INTEGER NOT NULL,
                field_idx INTEGER NOT NULL,
                value BLOB,
                FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx),
                FOREIGN KEY(field_idx) REFERENCES fields(field_idx)
            )
        """)

        # Create files table
        cursor.execute("""
            CREATE TABLE files (
                doc_idx INTEGER NOT NULL,
                filename TEXT NOT NULL,
                uid TEXT NOT NULL UNIQUE,
                orig_location TEXT NOT NULL,
                cached_location TEXT,
                type TEXT NOT NULL,
                parameters TEXT,
                FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx),
                PRIMARY KEY(doc_idx, filename, uid)
            )
        """)

        # Create deleted_docs table
        # Ids of documents that were removed from their last branch. These are
        # never re-used -- see _do_add_doc (DID-matlab issue #55).
        self._create_deleted_docs_table(cursor)

        self._commit()

    def do_run_sql_query(self, query_str, params=()):
        """Run one SQL statement and return its rows.

        A transient BUSY / READONLY failure is retried with backoff, as
        DID-matlab's run_sql_noOpen does -- but only when the connection had
        no uncommitted work before the statement ran. A retry rolls back, and
        a READONLY retry reopens the connection; either would silently discard
        statements earlier in an open transaction, so in that case the error
        is reported and raised at once for the caller to roll back.
        """
        was_idle = not self.dbid.in_transaction
        try:
            return self._fetchall(query_str, params)
        except sqlite3.Error as err:
            if not (was_idle and self._is_retryable_sql_error(err)):
                self._report_sql_error(err)
                raise
            last_err = err

        # Nothing was pending, and SQLite statements are atomic, so the failed
        # statement applied nothing: re-running it is safe even for an INSERT.
        for attempt, backoff in enumerate(_RETRY_BACKOFFS, start=1):
            reopen_first = self._is_readonly_error(last_err)
            if self.debug:
                print(
                    f"DID:SQLITEDB retryable SQL error (attempt {attempt}/"
                    f"{len(_RETRY_BACKOFFS)}, {backoff:g} s backoff): "
                    f"{str(last_err).strip()}\n"
                    f"{self._collect_readonly_diagnostics()}",
                    file=sys.stderr,
                )
            time.sleep(backoff)

            # A persistent READONLY on a writable file and directory is a
            # poisoned connection (SQLITE_READONLY_DBMOVED: SQLite believes
            # the file moved out from under the handle). Waiting never clears
            # it; reopening does. A plain BUSY needs only the wait, plus a
            # rollback of the implicit BEGIN so the retry starts afresh.
            if reopen_first or self.dbid is None:
                self._reopen_db()
            elif self.dbid.in_transaction:
                self.dbid.rollback()
            if self.dbid is None:
                continue  # reopen failed; wait and try again

            try:
                rows = self._fetchall(query_str, params)
            except sqlite3.Error as err:
                last_err = err
            else:
                if self.debug:
                    print(
                        f"DID:SQLITEDB query succeeded on retry attempt "
                        f"{attempt}/{len(_RETRY_BACKOFFS)}"
                        + (" (after reopen)" if reopen_first else ""),
                        file=sys.stderr,
                    )
                return rows
            if not self._is_retryable_sql_error(last_err):
                break  # a different, non-transient error - stop retrying

        if self.dbid is None:
            # Leave the object usable for the next call if the path recovers.
            with contextlib.suppress(Exception):
                self._open_db()
        self._report_sql_error(last_err)
        raise last_err

    def _fetchall(self, query_str, params=()):
        cursor = self.dbid.cursor()
        cursor.execute(query_str, params)
        return cursor.fetchall()

    def _commit(self):
        """Commit, recovering a transient BUSY / READONLY failure.

        In DID-matlab every statement autocommits, so the statement retry in
        run_sql_noOpen covers the write that reaches the disk. Here writes
        are grouped into a transaction and reach the disk at COMMIT, so the
        commit is retried too. A BUSY commit is waited out on the same
        connection while the transaction is still open. A READONLY commit
        inside a journaled write operation is recovered the way MATLAB
        recovers a READONLY statement -- reopen, then re-run -- except that
        what is re-run is the whole transaction, replayed and verified by
        :meth:`_recover_transaction`, and then committed.
        """
        if self._journal is not None and self._file_was_replaced():
            # Not on this handle: it would commit to the replaced file.
            self._recover_transaction(self._replaced_file_error(), commit=True)
            self._journal_committed()
            return
        try:
            self.dbid.commit()
        except sqlite3.Error as err:
            last_err = err
        else:
            self._journal_committed()
            return
        if self._journal is not None and self._is_readonly_error(last_err):
            self._recover_transaction(last_err, commit=True)
            self._journal_committed()
            return
        if self._is_retryable_sql_error(last_err):
            for attempt, backoff in enumerate(_RETRY_BACKOFFS, start=1):
                if not self.dbid.in_transaction:
                    break
                if self.debug:
                    print(
                        f"DID:SQLITEDB retryable COMMIT error (attempt "
                        f"{attempt}/{len(_RETRY_BACKOFFS)}, {backoff:g} s "
                        f"backoff): {str(last_err).strip()}",
                        file=sys.stderr,
                    )
                time.sleep(backoff)
                try:
                    self.dbid.commit()
                except sqlite3.Error as err:
                    last_err = err
                else:
                    self._journal_committed()
                    return
                if not self._is_retryable_sql_error(last_err):
                    break
        self._report_sql_error(last_err)
        raise last_err

    def _file_identity(self):
        """``(st_dev, st_ino)`` of the file at the path, or None if absent."""
        try:
            info = os.stat(self.connection)
        except OSError:
            return None
        return (info.st_dev, info.st_ino)

    def _file_was_replaced(self):
        """Does the path no longer name the file this connection opened?

        SQLite checks this itself (SQLITE_READONLY_DBMOVED) only when a write
        transaction starts. A file replaced after that -- by a sync client, a
        restore, a copy-then-rename -- is not noticed, and COMMIT succeeds
        into the old file, which no longer has a name: the write is lost
        without an error. Checked just before COMMIT, it is caught while
        nothing has been committed. It is the same comparison SQLite's unix
        VFS makes (the path's inode against the open file's), costs one
        stat, and a deleted file counts as replaced. A volume that reports
        unstable inode numbers can trip it falsely; that costs one replay
        onto the same file, since nothing was committed yet.
        """
        if self._db_identity is None:
            return False
        return self._file_identity() != self._db_identity

    def _replaced_file_error(self):
        # Worded as SQLite words DBMOVED, so it is classified the same way.
        return sqlite3.OperationalError(
            "attempt to write a readonly database (the database file at "
            f"{self.connection!r} was replaced while it was open, so this "
            "connection would have committed to a file that no longer has a "
            "name)"
        )

    def _rollback(self):
        """Roll back, if there is still a connection to roll back.

        After a recovery whose every reopen failed there may be none; the
        error that caused that is the one the caller should see.
        """
        if self.dbid is not None:
            self.dbid.rollback()

    def _journal_cursor(self):
        """A cursor for a write operation: recording, when one is journaled."""
        if self._journal is None:
            return self.dbid.cursor()
        return _JournalCursor(self, self.dbid.cursor())

    def _journal_committed(self):
        """What the journal held is on disk now; a later replay starts here."""
        if self._journal is None:
            return
        for entry in self._journal:
            entry["proxy"]._entry = None
        self._journal.clear()
        pending, self._pending_deletes = self._pending_deletes or [], []
        for path, what in pending:
            self._delete_original(path, what)

    def _delete_after_commit(self, path, what):
        """Delete an ingested original once the write operation commits.

        Inside a write operation the delete is queued and done by
        _journal_committed, so a rollback leaves the only copy of the file
        where it was. Outside one it happens at once, as before. Mirrors
        DID-matlab's sqlitedb.deleteAfterCommit.
        """
        if self._pending_deletes is None:
            self._delete_original(path, what)
        else:
            self._pending_deletes.append((path, what))

    @staticmethod
    def _delete_original(path, what):
        try:
            os.remove(path)
        except FileNotFoundError:
            # Already gone (two files can share one original): the point is
            # that it must not be there afterwards. MATLAB's commitWrite
            # suppresses MATLAB:DELETE:FileNotFound for the same case.
            pass
        except OSError as error:
            warnings.warn(
                f'Ingested {what} but could not delete the original "{path}": '
                f"{error}",
                stacklevel=3,
            )

    def _record(self, proxy, method, sql, params):
        entry = {
            "method": method,
            "sql": sql,
            "params": params,
            "rowcount": proxy._cursor.rowcount,
            # Only an INSERT that inserted has a lastrowid of its own: after
            # executemany, or an INSERT OR IGNORE that ignored, sqlite3
            # reports a stale value (or None) that no later statement used.
            "lastrowid": (
                proxy._cursor.lastrowid
                if method == "execute"
                and _is_insert(sql)
                and proxy._cursor.rowcount > 0
                else None
            ),
            "rows": [],
            "complete": False,
            "proxy": proxy,
        }
        self._journal.append(entry)
        proxy._entry = entry

    def _journaled_execute(self, proxy, method, sql, params):
        """Run one statement of a journaled write operation.

        A READONLY failure here is the poisoned-handle case (typically
        SQLITE_READONLY_DBMOVED), which SQLite reports at the first write of
        a transaction. It is recovered by :meth:`_recover_transaction`, which
        replays what came before on a fresh connection and then runs this
        statement there.
        """
        try:
            getattr(proxy._cursor, method)(sql, params)
        except sqlite3.Error as err:
            if not self._is_readonly_error(err):
                raise
            self._recover_transaction(err, pending=(proxy, method, sql, params))
            return proxy
        self._record(proxy, method, sql, params)
        return proxy

    def _replay(self, journal):
        """Re-run ``journal`` on the current connection, checking each result.

        Every statement must do what it did the first time: the same
        rowcount, the same lastrowid for an INSERT (later statements were
        built from it), and the same rows for anything the caller read. Any
        difference means the database changed in between -- another writer,
        or a different file now at the path -- and the decisions the
        operation made from those reads no longer hold, so the replay is
        refused rather than committed.
        """
        cursors = []
        for number, entry in enumerate(journal, start=1):
            cursor = self.dbid.cursor()
            try:
                getattr(cursor, entry["method"])(entry["sql"], entry["params"])
            except sqlite3.Error as err:
                if self._is_retryable_sql_error(err):
                    raise  # transient: the next attempt may get through
                # It succeeded the first time; failing now (a UNIQUE row
                # that is already there, a missing table) is a difference.
                raise _ReplayDiverged(
                    f"statement {number} failed on replay: {err}"
                ) from err
            if cursor.rowcount != entry["rowcount"]:
                raise _ReplayDiverged(
                    f"statement {number} changed {cursor.rowcount} rows on "
                    f"replay, {entry['rowcount']} the first time"
                )
            if entry["lastrowid"] is not None and (
                cursor.lastrowid != entry["lastrowid"]
            ):
                raise _ReplayDiverged(
                    f"statement {number} inserted row {cursor.lastrowid} on "
                    f"replay, {entry['lastrowid']} the first time"
                )
            seen = entry["rows"]
            if entry["complete"]:
                again = [tuple(row) for row in cursor.fetchall()]
            elif seen:
                again = [tuple(row) for row in cursor.fetchmany(len(seen))]
            else:
                again = []
            if again != seen:
                raise _ReplayDiverged(
                    f"statement {number} read different rows on replay"
                )
            cursors.append((entry, cursor))
        return cursors

    def _recover_transaction(self, err, pending=None, commit=False):
        """Reopen, replay the journaled transaction, and carry on.

        DID-matlab reopens the connection before retrying a READONLY
        statement (da83fa3): a poisoned handle -- SQLITE_READONLY_DBMOVED,
        SQLite believing the file moved out from under it -- is cleared by
        reopening, never by waiting. mksqlite autocommits each statement, so
        there the failed statement is all there is to re-run. Here it is part
        of a transaction whose earlier statements die with the handle, so all
        of them are replayed on the fresh connection and verified (see
        :meth:`_replay`) before the operation continues. Only SQL is
        replayed: files the operation already copied stay where they are,
        which is why the operation itself is not simply run again.

        ``pending`` is the statement that failed, run after the replay; with
        ``commit`` the replayed transaction is committed. On the same backoff
        schedule as the statement retry; a replay that diverges is never
        retried, and the original error is raised.
        """
        journal = self._journal
        last_err = err
        for attempt, backoff in enumerate(_RETRY_BACKOFFS, start=1):
            if self.debug:
                print(
                    f"DID:SQLITEDB readonly inside a write operation (attempt "
                    f"{attempt}/{len(_RETRY_BACKOFFS)}, {backoff:g} s backoff): "
                    f"reopening and replaying {len(journal)} statement(s)",
                    file=sys.stderr,
                )
            # Let go of the dead transaction first, so its locks do not hold
            # other writers up for the length of the backoff.
            if self.dbid is not None:
                with contextlib.suppress(sqlite3.Error):
                    self.dbid.rollback()
            time.sleep(backoff)
            if not os.path.exists(self.connection):
                # Reopening would create a new, empty database at the path
                # and replay into it. The database is gone; say so. Mirrors
                # DID-matlab's DID:SQLITEDB:DatabaseFileGone.
                print(
                    f"DID:SQLITEDB the database file {self.connection} no "
                    "longer exists; not reopening it, which would create a "
                    "new, empty database there. Nothing from this operation "
                    "was written.",
                    file=sys.stderr,
                )
                raise err from FileNotFoundError(self.connection)
            # Suspended while reopening: _open_db commits on its own.
            self._journal = None
            try:
                self._reopen_db()
                if self.dbid is None:
                    continue
                try:
                    cursors = self._replay(journal)
                    if commit:
                        if self._file_was_replaced():
                            # Replaced again while we replayed: go round.
                            raise self._replaced_file_error()
                        self.dbid.commit()
                except _ReplayDiverged as diverged:
                    with contextlib.suppress(sqlite3.Error):
                        self.dbid.rollback()
                    print(
                        "DID:SQLITEDB could not recover the write operation: "
                        f"{diverged}. Nothing from it was written.",
                        file=sys.stderr,
                    )
                    self._report_sql_error(err)
                    raise err from diverged
                except sqlite3.Error as replay_err:
                    with contextlib.suppress(sqlite3.Error):
                        self.dbid.rollback()
                    last_err = replay_err
                    if not self._is_retryable_sql_error(replay_err):
                        break
                    continue
            finally:
                self._journal = journal

            # The caller's cursors now point at the fresh connection, each at
            # the result of its last statement.
            for entry, cursor in cursors:
                entry["proxy"]._cursor = cursor
            if pending is not None:
                proxy, method, sql, params = pending
                proxy._cursor = self.dbid.cursor()
                try:
                    getattr(proxy._cursor, method)(sql, params)
                except sqlite3.Error as pending_err:
                    if not self._is_readonly_error(pending_err):
                        # An ordinary failure of the statement itself, as it
                        # would have been the first time: the transaction
                        # stays open for the caller, just as SQLite leaves it.
                        raise
                    last_err = pending_err
                    continue
                self._record(proxy, method, sql, params)
            if self.debug:
                print(
                    f"DID:SQLITEDB write operation recovered on attempt "
                    f"{attempt}/{len(_RETRY_BACKOFFS)} (after reopen)",
                    file=sys.stderr,
                )
            return
        if self.dbid is None:
            # Every reopen failed. Leave a connection behind if the path has
            # come back, so the caller's rollback does not hide this error.
            with contextlib.suppress(sqlite3.Error, OSError):
                self._open_db()
        self._report_sql_error(last_err)
        raise last_err

    def _reopen_db(self):
        """Close and reopen the connection, leaving dbid None if that fails."""
        with contextlib.suppress(Exception):
            self._close_db()
        self.dbid = None  # force a real reopen even if close failed
        try:
            if self.debug:
                print(
                    "DID:SQLITEDB reopening connection before retry "
                    "(readonly - likely DBMOVED)",
                    file=sys.stderr,
                )
            self._open_db()
        except (sqlite3.Error, OSError):
            # Fall through and let the retry (and the final report) run.
            self.dbid = None

    @staticmethod
    def _is_retryable_sql_error(err):
        """Is this a transient SQLite error worth retrying?

        True for SQLITE_BUSY / SQLITE_LOCKED ("database is locked",
        "database table is locked") and the SQLITE_READONLY family ("attempt
        to write a readonly database"). On a slow drive READONLY's usual
        cause is a hot journal awaiting rollback or a transient lock, both of
        which clear on their own; a permanently read-only file or mount
        simply exhausts the retries and is raised. Only sqlite3 errors count:
        a Python exception whose text mentions "read only" is not one.
        """
        if not isinstance(err, sqlite3.Error):
            return False
        msg = str(err).strip().lower()
        return SQLiteDB._is_readonly_error(err) or any(
            phrase in msg
            for phrase in (
                "database is locked",
                "database is busy",
                "database table is locked",
            )
        )

    @staticmethod
    def _is_readonly_error(err):
        """Is this the SQLITE_READONLY family specifically?

        Distinguished from BUSY / locked because READONLY on a writable file
        means the connection is poisoned (typically SQLITE_READONLY_DBMOVED)
        and must be reopened, not merely waited out.
        """
        if not isinstance(err, sqlite3.Error):
            return False
        msg = str(err).strip().lower()
        return any(p in msg for p in ("readonly", "read-only", "read only"))

    def _collect_readonly_diagnostics(self):
        """Filesystem state behind an "attempt to write a readonly database".

        SQLITE_READONLY has extended causes that need opposite fixes. This
        tells them apart without relying on the extended result code:
        a hot rollback journal / WAL beside the database (an interrupted
        write), a database directory that is no longer writable (a drive
        remounted read-only), or a read-only database file. Runs only on the
        error path.
        """
        filename = self.connection or ""
        lines = [
            f"  db file: {filename}",
            f"  time   : {datetime.now(timezone.utc).isoformat(timespec='milliseconds')}",
        ]

        # Hot journal / WAL siblings left next to the database
        for suffix in ("-journal", "-wal", "-shm"):
            with contextlib.suppress(OSError):
                if os.path.isfile(filename + suffix):
                    size = os.path.getsize(filename + suffix)
                    lines.append(f"  sibling {suffix} present ({size} bytes)")

        # Is the DB file itself writable?
        if os.path.exists(filename):
            lines.append(f"  db file UserWrite={int(os.access(filename, os.W_OK))}")

        # Is the DB directory still writable? Probe by creating a temp file.
        # This is the key discriminator for a drive that remounted read-only.
        db_dir = os.path.dirname(filename) or os.getcwd()
        try:
            with tempfile.NamedTemporaryFile(dir=db_dir, prefix=".did_write_probe_"):
                pass
            lines.append("  db dir writable: yes")
        except OSError as probe_err:
            lines.append(f"  db dir writable: NO ({probe_err})")

        return "\n".join(lines)

    def _report_sql_error(self, err):
        """For a BUSY / READONLY failure that survived the retries, print
        filesystem state so the cause can be identified (DID-matlab does the
        same, to stderr)."""
        if self._is_retryable_sql_error(err):
            print(
                "SQLite readonly/busy diagnostics:\n"
                + self._collect_readonly_diagnostics(),
                file=sys.stderr,
            )

    # The abstract methods from the Database class will be implemented here.
    # For brevity, I will start with a few key methods.

    def _do_get_branch_ids(self):
        rows = self.do_run_sql_query("SELECT DISTINCT branch_id FROM branches")
        return [row["branch_id"] for row in rows]

    @_journaled
    def _do_add_branch(self, branch_id, parent_branch_id):
        import time

        cursor = self._journal_cursor()

        # Handle empty string parent as NULL
        if parent_branch_id == "":
            parent_branch_id = None

        # Add the new branch
        cursor.execute(
            "INSERT INTO branches (branch_id, parent_id, timestamp) VALUES (?, ?, ?)",
            (branch_id, parent_branch_id, time.time()),
        )

        # Copy docs from parent branch
        if parent_branch_id:
            cursor.execute(
                "SELECT doc_idx FROM branch_docs WHERE branch_id = ?",
                (parent_branch_id,),
            )
            doc_indices = [row["doc_idx"] for row in cursor.fetchall()]
            for doc_idx in doc_indices:
                cursor.execute(
                    "INSERT OR IGNORE INTO branch_docs (branch_id, doc_idx, timestamp) VALUES (?, ?, ?)",
                    (branch_id, doc_idx, time.time()),
                )

        self._commit()

    def _do_get_doc_ids(self, branch_id=None):
        """Document ids on a branch, or -- with no branch -- in the database.

        The no-argument form is load-bearing: all_doc_ids() relies on it, and
        DID-matlab's do_get_doc_ids documents the same fallback. It looks dead
        from get_doc_ids, which validates its id first (issue #55), so do not
        remove it on that reading.
        """
        if branch_id:
            rows = self.do_run_sql_query(
                "SELECT d.doc_id FROM docs d JOIN branch_docs bd ON d.doc_idx = bd.doc_idx WHERE bd.branch_id = ?",
                (branch_id,),
            )
        else:
            rows = self.do_run_sql_query("SELECT doc_id FROM docs")
        return [row["doc_id"] for row in rows]

    def _get_field_idx(self, cursor, group_name, field_name):
        """Look up or create a field_idx for the given group and field.

        The field_name in the fields table uses the format '{group}.{field}',
        matching MATLAB's convention. Triple-underscores in column names from
        doc2sql are converted to dots.
        """
        # Convert ___ back to . for the stored field_name
        full_field_name = f"{group_name}.{field_name}".replace("___", ".")
        json_name = full_field_name.replace(".", "___")

        cache_key = (group_name, full_field_name)
        if cache_key in self._fields_cache:
            return self._fields_cache[cache_key]

        cursor.execute(
            "SELECT field_idx FROM fields WHERE field_name = ?", (full_field_name,)
        )
        row = cursor.fetchone()
        if row:
            field_idx = row["field_idx"]
        else:
            cursor.execute(
                "INSERT INTO fields (class, field_name, json_name, field_idx) VALUES (?, ?, ?, NULL)",
                (group_name, full_field_name, json_name),
            )
            field_idx = cursor.lastrowid

        self._fields_cache[cache_key] = field_idx
        return field_idx

    def _populate_doc_data(self, cursor, doc_idx, document_obj):
        """Flatten document via doc2sql and insert into fields/doc_data tables."""
        from .doc2sql import doc_to_sql

        meta_tables = doc_to_sql(document_obj)
        rows = []

        for table in meta_tables:
            group_name = table["name"]
            for col in table["columns"]:
                col_name = col["name"]
                if col_name == "doc_id":
                    continue  # skip doc_id columns
                field_idx = self._get_field_idx(cursor, group_name, col_name)
                value = col["value"]
                if value is None:
                    value = ""
                rows.append((doc_idx, field_idx, str(value)))

        if rows:
            cursor.executemany(
                "INSERT INTO doc_data (doc_idx, field_idx, value) VALUES (?, ?, ?)",
                rows,
            )

    @staticmethod
    def _matlab_compatible_props(props):
        """Return a deep copy of props with single-element lists unwrapped to scalars.

        MATLAB's jsonencode converts single-element cell arrays to scalars.
        This replicates that behavior so DID-matlab can read Python-created databases.
        """
        import copy

        props = copy.deepcopy(props)

        # Unwrap document_class.superclasses
        dc = props.get("document_class", {})
        sc = dc.get("superclasses")
        if isinstance(sc, list) and len(sc) == 1:
            dc["superclasses"] = sc[0]

        # Unwrap depends_on
        dep = props.get("depends_on")
        if isinstance(dep, list) and len(dep) == 1:
            props["depends_on"] = dep[0]

        # Unwrap files.file_info
        files = props.get("files")
        if isinstance(files, dict):
            fi = files.get("file_info")
            if isinstance(fi, list) and len(fi) == 1:
                files["file_info"] = fi[0]

        return props

    @staticmethod
    def _normalize_loaded_props(props):
        """Ensure superclasses, depends_on, file_info, and locations are always lists.

        Inverse of _matlab_compatible_props. Mutates and returns props.
        """
        dc = props.get("document_class", {})
        sc = dc.get("superclasses")
        if sc is not None and not isinstance(sc, list):
            dc["superclasses"] = [sc]

        dep = props.get("depends_on")
        if dep is not None and not isinstance(dep, list):
            props["depends_on"] = [dep]

        # Re-wrap files.file_info (but not locations, which may be a bare dict
        # from add_file in the Python API)
        files = props.get("files")
        if isinstance(files, dict):
            fi = files.get("file_info")
            if isinstance(fi, dict):
                files["file_info"] = [fi]

        return props

    @_journaled
    def _do_add_doc(
        self,
        document_obj,
        branch_id,
        OnDuplicate="error",
        custom_file_handler=None,
        **kwargs,
    ):
        import json
        import time

        on_dup = str(OnDuplicate).lower()
        if on_dup not in self._ON_DUPLICATE_CHOICES:
            raise ValueError(
                "OnDuplicate must be one of "
                f"{self._ON_DUPLICATE_CHOICES}; got {OnDuplicate!r}."
            )

        doc_id = document_obj.id()
        cursor = self._journal_cursor()

        # Validate branch existence BEFORE any INSERT. Previously the docs and
        # doc_data rows were inserted first and the missing branch was only
        # discovered by the branch_docs FOREIGN KEY check, which raised without
        # rolling back — leaving orphan rows in the open transaction that the
        # next successful add committed (a document belonging to no branch was
        # then reported present). Checking first means a bad branch never
        # inserts anything.
        cursor.execute("SELECT 1 FROM branches WHERE branch_id = ?", (branch_id,))
        if not cursor.fetchone():
            raise ValueError(f"Branch '{branch_id}' does not exist.")

        # A document id that was removed from its last branch is retired for
        # good and cannot be used again (DID-matlab issue #55, where MATLAB
        # raises DID:SQLITEDB:DELETED_DOC; ValueError is this class's
        # convention for the same kind of refusal).
        #
        # The reason here is not the one the issue gives. MATLAB re-used ids
        # kept their old doc_data rows, so a re-added document silently
        # inherited stale field values; that never happened in Python, which
        # has reclaimed those rows since #39. Ids are retired here because
        # they must not be re-used under DID's branch model, and because both
        # languages have to agree about whether this is legal on a database
        # file they share.
        if self._is_deleted_doc_id(cursor, doc_id):
            raise ValueError(
                f"Cannot add document '{doc_id}' - a document with this id was "
                f"previously removed from every branch. Document ids are never "
                f"re-used; generate a new id for a new document."
            )

        # Snapshot the fields cache so a rollback also unwinds any field_idx
        # entries _populate_doc_data added for fields whose INSERTs get rolled
        # back with the transaction (otherwise the cache would point at
        # non-existent field rows).
        cache_before = dict(self._fields_cache)

        try:
            cursor.execute("SELECT doc_idx FROM docs WHERE doc_id = ?", (doc_id,))
            row = cursor.fetchone()

            if row:
                doc_idx = row["doc_idx"]
            else:
                # Refuse a document whose series declares present members
                # but records no ingest_locations -- the shape a document
                # has after a round trip through storage. Only for a
                # document new to this database, and before any write.
                self._reject_series_without_ingest_locations(document_obj)

                # Strip series members' transient ingest paths from the
                # JSON before storing: member paths never persist.
                from ..document import Document

                props_for_storage = Document.strip_series_ingest_locations(
                    self._matlab_compatible_props(document_obj.document_properties)
                )
                json_code = json.dumps(props_for_storage)
                cursor.execute(
                    "INSERT INTO docs (doc_id, json_code, timestamp) VALUES (?, ?, ?)",
                    (doc_id, json_code, time.time()),
                )
                doc_idx = cursor.lastrowid

                # Populate fields and doc_data tables (matching MATLAB's doc2sql behavior)
                self._populate_doc_data(cursor, doc_idx, document_obj)

            # Is this document already on this branch? If so it is a duplicate.
            # DID-matlab's add_docs errors by default (OnDuplicate='error');
            # the previous Python code swallowed the branch_docs duplicate with
            # a bare ``pass``, silently keeping stale content. Honor OnDuplicate.
            cursor.execute(
                "SELECT 1 FROM branch_docs WHERE branch_id = ? AND doc_idx = ?",
                (branch_id, doc_idx),
            )
            if cursor.fetchone():
                if on_dup == "error":
                    raise ValueError(
                        f"Document '{doc_id}' is already in branch '{branch_id}'."
                    )
                if on_dup == "warn":
                    warnings.warn(
                        f"Document '{doc_id}' is already in branch '{branch_id}'; "
                        "leaving the existing entry unchanged.",
                        stacklevel=2,
                    )
                # 'ignore'/'warn': no-op (do not rewrite content). Commit so any
                # docs/doc_data rows inserted above for a brand-new doc_id are
                # persisted, then return.
                self._commit()
                return

            # Record the document's file locations. MATLAB's do_open_doc
            # resolves a file by querying `docs, files` -- it never reads the
            # locations back out of the document JSON -- so without these rows
            # every file in a Python-written document is unreachable from
            # MATLAB, including a plain local file that exists on disk.
            # Below the duplicate check: MATLAB 6edd765 likewise skips its
            # file-caching loop on the duplicate path, as duplicate work.
            self._populate_files(
                cursor, doc_idx, document_obj, custom_file_handler=custom_file_handler
            )

            cursor.execute(
                "INSERT INTO branch_docs (branch_id, doc_idx, timestamp) VALUES (?, ?, ?)",
                (branch_id, doc_idx, time.time()),
            )
            self._commit()
        except Exception:
            # Every failure path rolls back so no partial rows survive into the
            # next transaction, and the fields cache is restored to match.
            self._rollback()
            self._fields_cache = cache_before
            raise

    @staticmethod
    def _file_entries(document_obj):
        """Yield (filename, location_entry) for every location of every file."""
        files = document_obj.document_properties.get("files")
        if not isinstance(files, dict):
            return
        file_info = files.get("file_info")
        if isinstance(file_info, dict):
            file_info = [file_info]
        if not isinstance(file_info, list):
            return

        for info in file_info:
            if not isinstance(info, dict):
                continue
            name = info.get("name")
            if not isinstance(name, str) or not name:
                continue
            locations = info.get("locations")
            if isinstance(locations, dict):
                locations = [locations]
            if not isinstance(locations, list):
                continue
            for entry in locations:
                if isinstance(entry, dict) and entry.get("location"):
                    yield name, entry

    def _ingest_location(self, filename, entry, custom_file_handler, document_id=""):
        """Ingest a location marked `ingest`; return the local copy's path.

        MATLAB's do_add_doc walks every location whose ``ingest`` flag is set
        and puts a copy at ``<FileDir>/<uid>``, recording that path in
        files.cached_location. A local location is copied; anything else is
        handed to ``custom_file_handler(dest_path, source_path)``, exactly as
        open_doc's remote pass does. DID downloads nothing itself -- a
        downstream package supplies remote retrieval through the handler.

        On success, ``delete_original`` deletes the source, as MATLAB does.
        Never for a remote location: MATLAB guards that with a '://' check,
        since a remote location is not ours to remove.

        Returns "" -- an empty cached_location -- when there is nothing to
        ingest or the ingest failed. A failure warns rather than raises, as
        MATLAB's does: the document is still added, its orig_location is still
        recorded, and open_doc can reach the file later. Failing the whole add
        would lose the document over a file MATLAB is willing to add without.
        """
        location = str(entry.get("location", ""))
        uid = str(entry.get("uid", ""))
        if not entry.get("ingest") or not location or not uid:
            return ""

        # A document ingested from a cloud pull is attacker-controlled JSON:
        # a crafted ``uid`` or ``location`` could otherwise let this method
        # copy to (or over) an arbitrary file. Refuse -- do not sanitise --
        # so a corrupt document stays out of the database rather than being
        # partly written. See DID-python issue #58.
        if not _is_safe_uid(uid):
            raise ValueError(
                f'Refusing to ingest "{filename}": uid {uid!r} is not a '
                f"plain filename (no path separators, no '.' or '..', no "
                f"empty basename). See DID-python issue #58."
            )

        is_remote = self._is_remote_location(location, entry.get("location_type"))
        if is_remote and custom_file_handler is None:
            warnings.warn(
                f'The file "{filename}" location "{location}" is marked for '
                f"ingestion but is not a local path, and no "
                f"custom_file_handler was supplied. DID does not download "
                f"files itself; a downstream package supplies retrieval "
                f"through custom_file_handler. The document is added with no "
                f"ingested copy. See PORTING_INSTRUCTIONS.md.",
                stacklevel=2,
            )
            return ""

        file_dir = self._file_dir()
        dest_path = os.path.join(file_dir, uid)
        # A relative location is rebased against the database directory, the
        # same resolution open_doc uses -- otherwise a document written with a
        # relative path would be ingested from the process's cwd. The source
        # is caller-supplied ("add this file to my DB"); the destination is
        # already contained under <FileDir>/<uid> by the _is_safe_uid check
        # above, so a source outside db_dir is a normal ingest workflow, not
        # an attack. See DID-python issue #60.
        if is_remote:
            source_path = location
        else:
            source_path = self._resolve_local(location)

        if not is_remote and os.path.abspath(source_path) == os.path.abspath(dest_path):
            # Already the ingested copy: adding the same document again (to a
            # second branch, say) reaches this with the location rewritten to
            # <FileDir>/<uid>. Copying a file onto itself fails, and honoring
            # delete_original here would delete the only copy there is.
            return dest_path

        try:
            os.makedirs(file_dir, exist_ok=True)
            # Clear any earlier copy first, as open_doc does for its download:
            # otherwise a handler that produced nothing would be indis-
            # tinguishable from one that succeeded, and a stale file would be
            # recorded as this document's ingested copy.
            if os.path.exists(dest_path):
                os.remove(dest_path)
            if is_remote:
                # The DID-matlab#186 context, as MATLAB's do_add_doc sends it
                # (sqlitedb.m:567). seriesName is empty here: this is an
                # ordinary file_info location, not a series member. A
                # two-argument handler still gets two arguments -- that is
                # what _dispatch_custom_file_handler decides. See
                # DID-python#71.
                self._dispatch_custom_file_handler(
                    custom_file_handler,
                    dest_path,
                    location,
                    {
                        "documentId": document_id,
                        "filename": filename,
                        "seriesName": "",
                        "uid": uid,
                        "mode": "add",
                    },
                )
            else:
                shutil.copyfile(source_path, dest_path)
        except Exception as error:  # noqa: BLE001 - reported as a warning
            warnings.warn(
                f'Failed to ingest "{filename}" from "{location}": {error}',
                stacklevel=2,
            )
            return ""

        if not os.path.isfile(dest_path):
            warnings.warn(
                f'Failed to ingest "{filename}" from "{location}": '
                f'{"custom_file_handler did not produce a file" if is_remote else "no file was produced"} '
                f'at "{dest_path}"',
                stacklevel=2,
            )
            return ""

        # Only now, with a copy safely in place. MATLAB deletes the original
        # solely on the success branch, for the same reason.
        if entry.get("delete_original") and not is_remote:
            self._delete_after_commit(source_path, f'"{filename}"')
        return dest_path

    def _populate_files(self, cursor, doc_idx, document_obj, custom_file_handler=None):
        """Insert one `files` row per file location, mirroring MATLAB.

        Columns match MATLAB's insert exactly: doc_idx, filename, uid,
        orig_location, cached_location, type, parameters.

        cached_location is the ingested copy's path, which for now only a
        non-local location marked ``ingest`` and retrieved through
        ``custom_file_handler`` produces; see _ingest_location.

        INSERT OR IGNORE because a document added to a second branch reaches
        this path again with the same (doc_idx, filename, uid) primary key.

        Series members are also copied to ``<FileDir>/<uid>`` here, using
        the same machinery as ``file_info`` locations, but NO row is
        inserted for them: resolution goes through the manifest. This
        mirrors MATLAB's ``do_add_doc``.
        """
        self._ingest_series_members(document_obj, custom_file_handler)
        for name, entry in self._file_entries(document_obj):
            cached_location = self._ingest_location(
                name, entry, custom_file_handler, document_obj.id()
            )
            cursor.execute(
                "INSERT OR IGNORE INTO files "
                "(doc_idx, filename, uid, orig_location, cached_location, "
                "type, parameters) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    doc_idx,
                    name,
                    str(entry.get("uid", "")),
                    str(entry.get("location", "")),
                    cached_location,
                    str(entry.get("location_type", "file")),
                    str(entry.get("parameters", "")),
                ),
            )

    # --- SQL-based search (matching MATLAB's database.m) ---

    def search(self, query_obj, branch_id=None):
        """Search using SQL queries against doc_data, matching MATLAB's behavior.

        Overrides Database.search, so the branch guard has to be repeated here
        -- fixing only the base class would have left this path unchanged
        (issue #55).
        """
        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)

        search_params = query_obj.to_search_structure()

        # Register regexp function for sqlite
        self.dbid.create_function("regexp", 2, _sqlite_regexp)

        doc_ids = self._search_doc_ids(search_params, branch_id)
        return doc_ids

    def _search_doc_ids(self, search_struct, branch_id, candidates=None):
        """Recursively search for doc_ids matching the search structure.

        Matches MATLAB's search_doc_ids: struct arrays are AND'd, 'or' operations
        are unioned, leaf queries go through SQL.

        If *candidates* (a list of document ids) is given, only those documents
        are considered, so the result is a subset of them. Once one term of an
        AND has narrowed the result to at most ``_MAX_SEARCH_CANDIDATES``
        documents, the remaining terms are checked against those documents
        only. The terms of an AND are searched in this order (see
        ``_search_rank``): base.id lookups first, as they match at most a few
        documents; then the other terms, in the order given; then
        base.session_id terms, as one session usually holds most of a
        database; and last the terms that test every value they see
        (``_SCAN_OPERATIONS``). So ``isa daqsystem AND base.session_id = S AND
        base.name regexp ...`` finds the few daqsystem documents, then checks
        their session, then runs the regexp on their names only -- instead of
        each term returning, or testing, every document of the branch.

        Restricting to candidates needs the search indexes; without them
        (``_search_indexes_ready`` false) every term is searched over the whole
        branch, as before. Mirrors DID-matlab's did.database/search_doc_ids.
        """
        if not self._search_indexes_ready:
            candidates = None
        if candidates is not None and not candidates:
            return []

        if isinstance(search_struct, list):
            if not search_struct:
                return []
            # AND: intersect results from all sub-queries, the most selective
            # first (sorted is stable: the given order is kept within a rank)
            result = None
            for item in sorted(search_struct, key=_search_rank):
                ids = self._search_doc_ids(item, branch_id, candidates)
                if result is None:
                    result = set(ids)
                else:
                    result &= set(ids)
                if not result:
                    return []  # no document matches every term - skip the rest
                if len(result) <= _MAX_SEARCH_CANDIDATES:
                    candidates = list(result)
            return list(result)

        if not isinstance(search_struct, dict):
            return []

        operation = search_struct.get("operation", "")
        negation = False
        op = operation
        if op.startswith("~"):
            negation = True
            op = op[1:]
        op_lower = op.lower()

        # The documents a negated term is the complement within
        if candidates is not None:
            universe = candidates
        else:
            universe = None  # every document of the branch, fetched on demand

        if op_lower == "or":
            is_lookup, lookup_ids = _base_id_lookup_values(search_struct)
            if is_lookup and self._search_indexes_ready:
                # base.id lookups ORed together: search all the ids at once
                return self._search_base_ids(lookup_ids, branch_id, candidates)
            # OR: union results from param1 and param2
            p1 = search_struct.get("param1")
            p2 = search_struct.get("param2")
            ids1 = self._search_doc_ids(p1, branch_id, candidates) if p1 else []
            ids2 = self._search_doc_ids(p2, branch_id, candidates) if p2 else []
            result = list(set(ids1) | set(ids2))
            if negation:
                if universe is None:
                    universe = self._do_get_doc_ids(branch_id)
                result = list(set(universe) - set(result))
            return result

        # Leaf query: build SQL and execute
        try:
            sql_clause = self._query_struct_to_sql_str(
                search_struct, restricted=candidates is not None
            )
        except (ValueError, TypeError):
            # A numeric operation (exact_number/lessthan/greaterthan/...) was
            # given a non-numeric param1, so the float() conversion failed.
            # Fall back to brute force rather than aborting the whole search.
            return self._brute_force_search(search_struct, branch_id, candidates)
        if sql_clause is None:
            # Fallback to brute-force for unsupported operations
            return self._brute_force_search(search_struct, branch_id, candidates)

        query = self._search_query_head(candidates) + f" AND {sql_clause}"

        try:
            rows = self.do_run_sql_query(query, (branch_id,))
            matched = [row["doc_id"] for row in rows]
        except sqlite3.OperationalError:
            # Fallback on SQL error
            return self._brute_force_search(search_struct, branch_id, candidates)

        if negation:
            if universe is None:
                universe = self._do_get_doc_ids(branch_id)
            return list(set(universe) - set(matched))

        return matched

    @staticmethod
    def _search_query_head(candidates=None):
        """The SELECT ... WHERE of a search query, before its term.

        Takes the branch id as its one bound parameter. If *candidates* (a list
        of document ids) is given, only those documents are considered, and
        the tables are joined with CROSS JOIN, which makes SQLite loop over
        them in the order written: each candidate is found through the unique
        docs.doc_id index, and its doc_data rows through doc_data_doc_idx,
        named with INDEXED BY. Left to choose, the query planner (which has no
        table statistics to go on) can start from every document of the
        branch, or reach doc_data through an index on the searched value --
        for base.session_id, one row per document in the session, for every
        candidate. Mirrors DID-matlab's did.database/get_sql_query_str.
        """
        if candidates is None:
            tables = "docs, branch_docs, doc_data, fields"
        else:
            tables = (
                "docs CROSS JOIN branch_docs CROSS JOIN fields "
                "CROSS JOIN doc_data INDEXED BY doc_data_doc_idx"
            )
        query = (
            f"SELECT DISTINCT docs.doc_id FROM {tables} "
            "WHERE docs.doc_idx = doc_data.doc_idx "
            "AND docs.doc_idx = branch_docs.doc_idx "
            "AND branch_docs.branch_id = ? "
            "AND fields.field_idx = doc_data.field_idx"
        )
        if candidates is not None:
            query += f" AND docs.doc_id IN ({_sql_string_list(candidates)})"
        return query

    def _search_base_ids(self, ids, branch_id, candidates=None):
        """Find the documents whose base.id is one of *ids*.

        Equivalent to ORing a base.id exact_string query for each of *ids*, in
        one query per ``_MAX_SEARCH_CANDIDATES`` ids rather than one per id. A
        document's doc_id is its base.id, so *ids* also serve as the candidate
        documents, and are looked up through the docs.doc_id index. The
        doc_data condition is kept so that the result is exactly what the ORed
        exact_string queries return. Mirrors DID-matlab's
        did.database/search_base_ids.
        """
        ids = sorted(set(ids))
        if candidates is not None:
            allowed = set(candidates)
            ids = [i for i in ids if i in allowed]
        matched = []
        for start in range(0, len(ids), _MAX_SEARCH_CANDIDATES):
            chunk = ids[start : start + _MAX_SEARCH_CANDIDATES]
            query = (
                self._search_query_head(chunk)
                + " AND fields.field_name = 'base.id'"
                + f" AND doc_data.value IN ({_sql_string_list(chunk)})"
            )
            matched.extend(
                row["doc_id"] for row in self.do_run_sql_query(query, (branch_id,))
            )
        return matched

    def _query_struct_to_sql_str(self, search_struct, restricted=False):
        """Convert a single query struct to a SQL WHERE clause fragment.

        Returns None if the operation is not supported in SQL.
        Matches MATLAB's query_struct_to_sql_str.

        *restricted* is true when the query is limited to a list of candidate
        documents (see ``_search_query_head``); it changes how 'isa' is
        matched.
        """
        field = search_struct.get("field", "")
        operation = search_struct.get("operation", "")
        param1 = search_struct.get("param1")
        param2 = search_struct.get("param2")

        # Strip negation prefix (handled by caller)
        op = operation
        op = op.removeprefix("~")
        op_lower = op.lower()

        # The query field name is interpolated into the SQL text below (e.g.
        # fields.field_name = '<field>'); it cannot be a bound '?' parameter
        # because it also appears in LIKE patterns. Restrict it to the
        # dotted-identifier charset so a crafted field name cannot break out of
        # the quoting. An out-of-charset field returns None here and the caller
        # falls back to the (injection-free) brute-force search.
        if field and not _re.fullmatch(r"[A-Za-z0-9_.]+", field):
            return None

        if op_lower == "exact_string":
            return f"fields.field_name = '{field}' AND doc_data.value = '{_sql_escape(param1)}'"

        elif op_lower == "exact_string_anycase":
            return f"fields.field_name = '{field}' AND LOWER(doc_data.value) = LOWER('{_sql_escape(param1)}')"

        elif op_lower == "contains_string":
            # param1 is arbitrary user text: '%' and '_' in it must NOT act as
            # LIKE wildcards (otherwise 'spike_sort' would also match
            # 'spikeXsort'), which would diverge from the brute-force
            # ``param1 in value`` path. Escape the LIKE wildcards, add an ESCAPE
            # clause, and SQL-escape the surrounding literal. The bracketing
            # '%' are the real "contains" wildcards and stay unescaped.
            param_like = _sql_escape(_sql_like_escape(param1))
            return (
                f"fields.field_name = '{field}' AND doc_data.value "
                f"LIKE '%{param_like}%' ESCAPE '{_LIKE_ESCAPE_CHAR}'"
            )

        elif op_lower == "regexp":
            return f"fields.field_name = '{field}' AND regexp('{_sql_escape(param1)}', doc_data.value) IS NOT NULL"

        elif op_lower == "exact_number":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) = {float(param1)}"

        elif op_lower == "lessthan":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) < {float(param1)}"

        elif op_lower == "lessthaneq":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) <= {float(param1)}"

        elif op_lower == "greaterthan":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) > {float(param1)}"

        elif op_lower == "greaterthaneq":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) >= {float(param1)}"

        elif op_lower == "hasfield":
            # 'field' is charset-restricted above, but it may legitimately
            # contain '_', which is a LIKE wildcard. Escape LIKE wildcards in
            # the literal prefix and add an ESCAPE clause so a field name like
            # 'a_b' matches 'a_b[.subfield]' exactly, not 'axb'. The trailing
            # '.%' is a real wildcard and is left unescaped.
            field_like = _sql_like_escape(field)
            return (
                f"(fields.field_name = '{field}' "
                f"OR fields.field_name LIKE '{field_like}.%' ESCAPE '{_LIKE_ESCAPE_CHAR}')"
            )

        elif op_lower == "isa":
            # isa: match on meta.class (exact) OR meta.superclass (contains).
            # The meta.class branch is an exact string compare, so it only
            # needs SQL-literal escaping. The meta.superclass branch embeds the
            # class name inside a regexp() pattern; regex metacharacters in the
            # class name (e.g. '.') must be regex-escaped first, otherwise a
            # name like 'foo.bar' would also match 'fooxbar'. Anchor it as an
            # exact list-element match between the '(^|, )' / '(,|$)' delimiters.
            classname = _sql_escape(param1)
            classname_re = _sql_escape(
                _re.escape("" if param1 is None else str(param1))
            )
            superclass_pattern = f"'(^|, ){classname_re}(,|$)'"
            superclass_match = (
                f"regexp({superclass_pattern}, doc_data.value) IS NOT NULL"
            )
            if not restricted and self._search_indexes_ready:
                # Over a whole branch, running the regexp on every document's
                # superclass list took seconds (~5 s for 100,000 documents in
                # DID-matlab). Most documents share one of a few dozen
                # superclass lists, so match the regexp against the distinct
                # lists only -- read in order from the doc_data_field_value
                # index -- and then find the documents holding a matching list
                # through the same index. LIMIT -1 keeps SQLite from merging
                # the inner DISTINCT query into the outer one, which would run
                # the regexp on every row again. Negation is applied by the
                # caller, so this clause is always the positive match.
                superclass_match = (
                    "doc_data.value IN ("
                    "SELECT superclasses FROM ("
                    "SELECT DISTINCT sc.value AS superclasses "
                    "FROM doc_data sc, fields scf "
                    "WHERE scf.field_name = 'meta.superclass' "
                    "AND sc.field_idx = scf.field_idx LIMIT -1) "
                    f"WHERE regexp({superclass_pattern}, superclasses) IS NOT NULL)"
                )
            return (
                f"((fields.field_name = 'meta.class' AND doc_data.value = '{classname}') "
                f"OR (fields.field_name = 'meta.superclass' AND {superclass_match}))"
            )

        elif op_lower == "depends_on":
            # depends_on: search meta.depends_on using LIKE '%name,value;%'.
            # NOTE: this branch is currently unreachable — Query._resolve_single
            # rewrites every 'depends_on' into 'hasanysubfield_exact_string'
            # before it reaches here, so depends_on always brute-forces. The
            # LIKE-wildcard escaping below is applied defensively so that if the
            # resolution is ever changed to let this branch run, '_'/'%' in the
            # dependency name/value can't silently act as wildcards. The ','/';'
            # delimiters and the bracketing '%' are the real pattern structure
            # and stay unescaped.
            name = _sql_escape(_sql_like_escape(param1))
            value = _sql_escape(_sql_like_escape(param2))
            if param1 == "*":
                return (
                    "fields.field_name = 'meta.depends_on' AND doc_data.value "
                    f"LIKE '%,{value};%' ESCAPE '{_LIKE_ESCAPE_CHAR}'"
                )
            return (
                "fields.field_name = 'meta.depends_on' AND doc_data.value "
                f"LIKE '%{name},{value};%' ESCAPE '{_LIKE_ESCAPE_CHAR}'"
            )

        elif op_lower == "hasanysubfield_exact_string":
            # unreachable — see Query._resolve_single (resolved depends_on);
            # falls back to brute force
            return None

        elif op_lower == "hasanysubfield_contains_string":
            # Used by resolved isa - fall back to brute force
            return None

        elif op_lower == "hasmember":
            # hasmember on a stored value - fall back to brute force
            return None

        elif op_lower == "hassize" or op_lower == "partial_struct":
            return None

        return None

    def _brute_force_search(self, search_struct, branch_id, candidates=None):
        """Fall back to brute-force field_search for unsupported SQL operations.

        Loads and tests every document of the branch -- or, if *candidates*
        (a list of document ids) is given, only those.
        """
        from ..datastructures import field_search

        if candidates is not None:
            doc_ids = list(candidates)
        else:
            doc_ids = self._do_get_doc_ids(branch_id)
        docs = self.get_docs(doc_ids, OnMissing="ignore")
        if docs is None:
            docs = []
        if not isinstance(docs, list):
            docs = [docs]

        matched = []
        for doc in docs:
            if doc and field_search(doc.document_properties, search_struct):
                matched.append(doc.id())
        return matched

    def _do_get_doc(self, document_id, OnMissing="error", **kwargs):
        import json

        from ..document import Document

        row = self.do_run_sql_query(
            "SELECT json_code FROM docs WHERE doc_id = ?", (document_id,)
        )

        if row:
            json_code = row[0]["json_code"]
            doc_struct = json.loads(json_code)
            doc_struct = self._normalize_loaded_props(doc_struct)
            return Document(doc_struct)
        else:
            # Handle missing document
            if OnMissing == "warn":
                print(f"Warning: Document id '{document_id}' not found.")
                return None
            elif OnMissing == "ignore":
                return None
            else:
                raise ValueError(f"Document id '{document_id}' not found.")

    def get_docs(self, document_ids, branch_id=None, OnMissing="error", **kwargs):
        """Bulk-fetch documents in a single SQL query.

        Overrides the base class one-at-a-time loop for efficiency.
        """
        import json

        from ..document import Document

        is_single = isinstance(document_ids, str)
        if is_single:
            document_ids = [document_ids]

        if not document_ids:
            return [] if not is_single else None

        # Fetch the requested docs in ONE indexed query, restricting to the
        # branch via a JOIN.
        #
        # PERF: the previous code fetched ALL of the branch's doc_ids
        # (``set(self.get_doc_ids(branch_id))``, O(total_docs)) and filtered
        # the small ``document_ids`` list in Python. NDI's ``epochtable``
        # calls ``get_docs`` once per epoch, so that was O(epochs x
        # total_docs) — a single ``getprobes`` took minutes on a large cloud
        # dataset (the live NDI spike-sort hung here). The branch JOIN +
        # ``doc_id IN (...)`` is O(len(document_ids)) using the branch_docs
        # and docs indexes. Branch membership is enforced by the JOIN; docs
        # not in the branch simply aren't returned and are handled by the
        # OnMissing pass below (same behaviour as before).
        # Chunk the IN-list: SQLite caps host parameters per statement
        # (SQLITE_MAX_VARIABLE_NUMBER — 999 on older builds), so a get_docs
        # over thousands of ids (e.g. a cross-document query on a large cloud
        # dataset) would raise "too many SQL variables". Batch under the limit
        # and accumulate; order is restored from doc_map below.
        _CHUNK = 900
        rows = []
        for _i in range(0, len(document_ids), _CHUNK):
            chunk = document_ids[_i : _i + _CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            if branch_id is not None:
                rows.extend(
                    self.do_run_sql_query(
                        f"SELECT d.doc_id, d.json_code FROM docs d "
                        f"JOIN branch_docs bd ON d.doc_idx = bd.doc_idx "
                        f"WHERE bd.branch_id = ? AND d.doc_id IN ({placeholders})",
                        (branch_id, *chunk),
                    )
                )
            else:
                rows.extend(
                    self.do_run_sql_query(
                        f"SELECT doc_id, json_code FROM docs WHERE doc_id IN ({placeholders})",
                        tuple(chunk),
                    )
                )

        # Build lookup dict
        doc_map = {}
        for row in rows:
            doc_struct = json.loads(row["json_code"])
            doc_struct = self._normalize_loaded_props(doc_struct)
            doc_map[row["doc_id"]] = Document(doc_struct)

        # Preserve original order
        docs = []
        for doc_id in document_ids:
            if doc_id in doc_map:
                docs.append(doc_map[doc_id])
            elif OnMissing == "error":
                raise ValueError(f"Document id '{doc_id}' not found.")
            elif OnMissing == "warn":
                print(f"Warning: Document id '{doc_id}' not found.")

        if is_single:
            return docs[0] if docs else None
        return docs

    def get_docs_by_branch(self, branch_id=None):
        """Return all documents on a branch."""
        if branch_id is None:
            branch_id = self.current_branch_id
        doc_ids = self.get_doc_ids(branch_id)
        return self.get_docs(doc_ids, OnMissing="ignore")

    @staticmethod
    def _is_remote_location(location, location_type=None):
        """Is this location one that has to be retrieved rather than opened?

        Anything carrying a URI scheme -- ndic://, https://, s3:// -- is
        remote. DID retrieves none of them itself; see open_doc.
        """
        if str(location_type or "").lower() in ("url", "ndicloud"):
            return True
        return "://" in location

    def _resolve_local(self, location):
        """Absolute path for a local location, rebased against the db dir."""
        if os.path.isabs(location):
            return location
        db_dir = os.path.dirname(os.path.abspath(self.connection))
        return os.path.join(db_dir, location)

    def _validated_local_path(self, location, purpose="location"):
        """``_resolve_local`` that refuses to escape the database directory.

        A location from an ingested document is attacker-reachable when the
        document itself was pulled from a cloud store; a value of
        ``"../../etc/passwd"`` (or an absolute path outside the db dir) would
        otherwise reach ``open()`` or ``shutil.copyfile()``. Only paths that
        resolve inside the database directory are accepted. See DID-python
        issue #58.

        Raises :class:`ValueError` on an unsafe location so a corrupt
        document stays out of the database rather than being partly written.
        """
        if not isinstance(location, str) or not location:
            raise ValueError(f"Refusing to use empty {purpose}.")
        db_dir = os.path.dirname(os.path.abspath(self.connection))
        resolved = self._resolve_local(location)
        resolved_abs = os.path.abspath(resolved)
        if not _is_within(db_dir, resolved_abs):
            raise ValueError(
                f"Refusing {purpose}: {location!r} resolves outside the "
                f"database directory {db_dir!r}. See DID-python issue #58."
            )
        return resolved_abs

    def _is_safe_local_location(self, location, location_type=None):
        """Would ``location`` open a file inside the db dir?

        Used to filter locations read back out of an older, unguarded
        database (or from a document JSON that predates the guard); a
        remote location is not a filesystem path and is left alone here.
        See DID-python issue #58.
        """
        if self._is_remote_location(location, location_type):
            return True
        try:
            self._validated_local_path(location)
        except ValueError:
            return False
        return True

    def _locations_from_files_table(self, doc_id, filename):
        """Locations for one file, read from the `files` table.

        This is where MATLAB keeps them, and it is the only place it looks:
        do_open_doc selects cached_location, orig_location, uid and type from
        `docs, files`. A MATLAB-written document that was ingested has had its
        original deleted, so the document JSON's location no longer exists and
        only these rows can find the file.

        Returns a list shaped like the document's own locations, so both
        sources feed the same resolution path.
        """
        try:
            cursor = self.dbid.cursor()
            rows = cursor.execute(
                "SELECT f.uid, f.orig_location, f.cached_location, f.type "
                "FROM docs d, files f "
                "WHERE d.doc_id = ? AND f.doc_idx = d.doc_idx AND f.filename = ?",
                (doc_id, filename),
            ).fetchall()
        except sqlite3.Error:
            return []

        cache = self._file_cache()

        locations = []
        for row in rows:
            uid = row["uid"]
            cached = row["cached_location"]
            # Cache and FileDir candidates both key off uid; an unsafe uid
            # (one written by an unguarded DID before issue #58) would build
            # a path outside the cache dir or outside FileDir, so skip those
            # candidates. The row's cached_location and orig_location are
            # still tried through the read-time containment filter below.
            uid_ok = _is_safe_uid(str(uid)) if uid else False
            # The global file cache comes first, as it does in MATLAB's
            # do_open_doc: a file retrieved from a remote location once is
            # kept there under its uid, so a second open of the same document
            # does not fetch it again.
            if uid_ok and cache is not None:
                locations.append(
                    {
                        "location": cache.full_path(str(uid)),
                        "location_type": "file",
                        "uid": uid,
                        "in_file_cache": True,
                        # Cache path is built from a uid we just validated,
                        # so it does not need to pass the db-dir containment
                        # filter -- the cache lives outside db_dir by design.
                        "_trusted_local": True,
                    }
                )
            # MATLAB stores an ingested copy at <FileDir>/<uid>, where FileDir
            # is <directory holding the .sqlite file>/files. It looks the copy
            # up by uid rather than by cached_location, so mirror that next.
            if uid_ok:
                locations.append(
                    {
                        "location": os.path.join(self._file_dir(), str(uid)),
                        "location_type": "file",
                        "uid": uid,
                        # <FileDir>/<uid> with a validated uid is inside
                        # db_dir by construction; skip the containment
                        # filter so it stays reachable even on filesystems
                        # where realpath returns something unexpected.
                        "_trusted_local": True,
                    }
                )
            if cached:
                locations.append({"location": cached, "location_type": "file"})
            if row["orig_location"]:
                locations.append(
                    {
                        "location": row["orig_location"],
                        "location_type": row["type"] or "file",
                        "uid": uid,
                    }
                )
        return locations

    def _file_dir(self):
        """MATLAB's FileDir: `files/` beside the database file."""
        return os.path.join(os.path.dirname(os.path.abspath(self.connection)), "files")

    def _do_cached_path_roots(self):
        """Override: this implementation keeps a uid-named file root of its own.

        Returned to :meth:`Database.cached_path_for_file` in the order MATLAB
        searches after the global file cache. Mirrors MATLAB
        ``did.implementations.sqlitedb/do_cachedPathRoots``.
        """
        return [self._file_dir()]

    @staticmethod
    def _file_cache():
        """The process-wide file cache, or None if it cannot be opened.

        MATLAB's do_open_doc calls did.common.getCache() unconditionally.
        Here a cache that cannot be created -- an unwritable home directory,
        a corrupt .fileCacheInfo -- must not make documents unopenable, since
        the cache is an optimization: without it every remote file is simply
        fetched again. So this degrades to None and open_doc carries on.
        """
        from ..common import get_cache

        try:
            return get_cache()
        except (OSError, ValueError, struct.error):
            return None

    @staticmethod
    def _valid_locations(info):
        """The document's locations for one file, always as a list of dicts.

        A document may list several locations for one file -- the shipped
        demoFile.json template carries a local path and a remote one for each
        -- and a single-location document stores a bare dict rather than a
        one-element list.
        """
        locations = info.get("locations")
        if isinstance(locations, dict):
            locations = [locations]
        elif not isinstance(locations, list):
            return []
        return [
            entry
            for entry in locations
            if isinstance(entry, dict)
            and isinstance(entry.get("location", ""), str)
            and entry.get("location")
        ]

    @staticmethod
    def _document_id(doc_id):
        """MATLAB's validate_doc_id: an id string, or a document to take it from.

        ``did.database/open_doc`` and ``exist_doc`` both accept "either the
        document id of a did.document, or a did.document object itself", so
        both accept it here too.
        """
        identify = getattr(doc_id, "id", None)
        return identify() if callable(identify) else doc_id

    def _locations_for_file(self, doc_id, filename, missing="error"):
        """Every recorded location for one of a document's files.

        The `files` table first, since that is the only place MATLAB records
        an ingested copy; then the document's own locations, which is all a
        database written before that table was populated will have.

        ``open_doc`` and ``exist_doc`` share this, so the two can never
        disagree about where a file is -- MATLAB's ``do_open_doc`` and
        ``check_exist_doc`` run the same ``docs, files`` join for the same
        reason. With ``missing="ignore"`` a document or filename that is not
        there returns None instead of raising, which is what ``exist_doc``
        needs: MATLAB's ``check_exist_doc`` reports tf=false for an unknown
        document rather than erroring.
        """
        doc = self.get_docs(
            doc_id, OnMissing="ignore" if missing == "ignore" else "error"
        )
        if not doc:
            if missing == "ignore":
                return None
            raise ValueError(f"Document {doc_id} not found.")

        _is_in, info, _ = doc.is_in_file_list(filename)
        if info is None:
            # A file series member has no file_info entry of its own --
            # membership is the manifest's to answer -- so is_in_file_list
            # accepts the name (via its series fallback) but hands us no
            # info to attach locations to. Return no locations here (an
            # empty list, not None): the caller (open_doc / exist_doc)
            # uses the document + manifest path directly for members.
            if doc.is_series_member(filename):
                # Attach the document so the caller can consult it without a
                # second fetch. Stashed as a tuple field on the empty list.
                empty = _SeriesMemberLocations(doc, filename)
                return empty
            if missing == "ignore":
                return None
            raise FileNotFoundError(f"File {filename} not found in document {doc_id}.")

        locations = self._locations_from_files_table(doc_id, filename)
        locations += self._valid_locations(info)
        # Defense in depth: filter out any local location that resolves
        # outside the database directory. Ingest-time checks refuse to
        # WRITE such a row, but a database written by an older DID may
        # already carry one; without this a `..`-crafted orig_location
        # would still let ``open_doc`` read /etc/passwd. Entries flagged
        # _trusted_local (uid-derived paths built from a validated uid)
        # skip the check -- the file cache lives outside db_dir by design.
        # See issue #58.
        return [
            entry
            for entry in locations
            if entry.get("_trusted_local")
            or self._is_safe_local_location(
                entry["location"], entry.get("location_type")
            )
        ]

    def _first_local_file(self, locations):
        """``(entry, path)`` for the first location already readable on disk.

        ``(None, None)`` when none is. This is MATLAB's first pass, before it
        tries to retrieve anything.
        """
        for entry in locations:
            location = entry["location"]
            if self._is_remote_location(location, entry.get("location_type")):
                continue
            resolved = self._resolve_local(location)
            if os.path.isfile(resolved):
                return entry, resolved
        return None, None

    def exist_doc(self, doc_id, filename):
        """Does `filename` exist for this document, and where?

        Mirrors MATLAB's ``did.database/exist_doc`` -> ``sqlitedb``'s
        ``check_exist_doc``, whose two outputs ``[tf, file_path]`` come back
        here as a tuple: whether the file is on disk, and its absolute path.
        Only the first matching location is reported, as in MATLAB.

        The path is None -- not "" -- when the file does not exist. MATLAB
        returns an empty char because that is its only empty string; Python
        has a value for "there is no path", and None cannot be mistaken for a
        usable relative path the way "" can (``os.path.join(dir, "")`` yields
        a directory).

        The lookup runs through the same resolution as ``open_doc``, so
        ``exist_doc`` is true exactly when ``open_doc`` would return a file
        without needing a ``custom_file_handler``.
        """
        if not str(filename or "").strip():
            # MATLAB: error('DID:SQLITEDB:open', 'The requested filename must
            # be specified in check_exist_doc()').
            raise ValueError("The requested filename must be specified in exist_doc().")

        locations = self._locations_for_file(
            self._document_id(doc_id), filename, missing="ignore"
        )
        if locations is None:
            return False, None

        if isinstance(locations, _SeriesMemberLocations):
            # Series members are resolved through the manifest without any
            # SQL. `exist_doc` reports on local state, so it never fetches
            # to answer: the answer is whatever `cached_path_for_file` gives.
            from ..database import _series_member_path

            tf, path = _series_member_path(
                locations.document_obj,
                locations.filename,
                self._do_cached_path_roots(),
            )
            return tf, (os.path.abspath(path) if tf else None)

        if not locations:
            return False, None

        entry, resolved = self._first_local_file(locations)
        if entry is None:
            return False, None
        return True, os.path.abspath(resolved)

    def open_doc(self, doc_id, filename, custom_file_handler=None):
        """Open a file belonging to a document.

        Local locations are opened directly. A remote location is handed to
        ``custom_file_handler(dest_path, source_path)``, which must produce a
        local file at ``dest_path``; this mirrors MATLAB's ``customFileHandler``
        name-value argument to ``do_open_doc``, and is how a downstream package
        supplies retrieval without DID depending on it. DID itself downloads
        nothing, in either language.

        Raises FileAccessError -- which carries MATLAB's error identifier and
        subclasses FileNotFoundError -- when no location can be reached.
        """
        from ..common import PathConstants
        from ..database import FileAccessError
        from ..file import ReadOnlyFileobj

        doc_id = self._document_id(doc_id)
        locations = self._locations_for_file(doc_id, filename)

        if isinstance(locations, _SeriesMemberLocations):
            return self._open_series_member(locations, custom_file_handler)

        # First pass: anything already on disk, as MATLAB does before it tries
        # to retrieve anything. exist_doc reports on exactly this pass.
        entry, resolved = self._first_local_file(locations)
        if entry is not None:
            if entry.get("in_file_cache"):
                # Record the use, or the cache would evict what is read
                # most as though it had never been read at all.
                cache = self._file_cache()
                if cache is not None:
                    with contextlib.suppress(OSError, ValueError):
                        cache.touch(str(entry["uid"]))
            return ReadOnlyFileobj(resolved)

        remote = [
            entry
            for entry in locations
            if self._is_remote_location(entry["location"], entry.get("location_type"))
        ]

        # Second pass: hand each remote location to the caller's retriever.
        # Each uid is fetched at most once per call; on the concurrent path a
        # per-uid lock in temppath serialises fetches across processes, matching
        # DID-matlab#185. A unique temp name per fetch guarantees no two writers
        # can share a scratch file, and the partial is deleted on any failure --
        # KeyboardInterrupt included -- so nothing is left to be mistaken for a
        # completed download.
        if remote and custom_file_handler is not None:
            temp_dir = PathConstants().temppath
            failures = []
            attempted_uids = set()
            for entry in remote:
                location = entry["location"]
                uid = entry.get("uid") or os.path.basename(location) or "did_download"
                uid_str = str(uid)
                if uid_str in attempted_uids:
                    continue  # each uid gets one attempt
                attempted_uids.add(uid_str)

                fetched_path, fetch_error = self._fetch_remote_to_cache(
                    uid_str,
                    location,
                    temp_dir,
                    custom_file_handler,
                    context={
                        "documentId": doc_id,
                        "filename": filename,
                        "uid": uid_str,
                        "mode": "open",
                    },
                )
                if fetched_path is not None:
                    return ReadOnlyFileobj(fetched_path)
                failures.append(f"{location}: {fetch_error}")
            raise FileAccessError(
                "DID:SQLITEDB:FileRetrieval:CustomHandlerFailed",
                f'The file "{filename}" in document "{doc_id}" could not be '
                f"retrieved: " + "; ".join(failures),
            )

        # Nothing was reachable. Raise rather than hand back a Fileobj over a
        # path that does not exist: Fileobj.fopen() swallows the OSError and
        # leaves fid None, so a caller that does not check it reads b"" and
        # sees no error at all.
        if remote and len(remote) == len(locations):
            raise FileAccessError(
                "DID:SQLITEDB:FileRetrieval:UnsupportedType",
                f'The file "{filename}" in document "{doc_id}" is only '
                f'available remotely ({remote[0]["location"]}) and no '
                f"custom_file_handler was supplied. DID does not download "
                f"files itself; a downstream package supplies retrieval "
                f"through custom_file_handler. See PORTING_INSTRUCTIONS.md.",
            )
        raise FileAccessError(
            "DID:SQLITEDB:open",
            f'The file "{filename}" in document "{doc_id}" cannot be accessed.',
        )

    def _reject_series_without_ingest_locations(self, document_obj):
        """Refuse a document whose series declares members with no ingest paths.

        Mirrors the MATLAB guard in ``sqlitedb/do_add_doc``: a document that
        went through the round trip through storage has empty
        ``ingest_locations`` on every series (stripped by
        :meth:`Document.strip_series_ingest_locations`), so adding it again to
        another database would silently record no series members. Refuse
        rather than add half a series.
        """
        for name in document_obj.series_names():
            _n, n_present = document_obj.series_count(name)
            if n_present > 0 and not document_obj.series_ingest_locations(name):
                raise ValueError(
                    f'Refusing to add document "{document_obj.id()}": series '
                    f'"{name}" declares {n_present} members but records no '
                    f"ingest_locations. This is the shape a document has "
                    f"after a round trip through storage; add it before "
                    f"stripping series ingest paths, or re-populate them."
                )

    def _ingest_series_members(self, document_obj, custom_file_handler):
        """Copy each series member into ``<FileDir>/<uid>``.

        Series members carry no ``files`` row: resolution goes through the
        manifest, so no ``INSERT`` here. Mirrors the member loop in MATLAB
        ``sqlitedb/do_add_doc``.
        """
        file_dir = self._file_dir()
        for name in document_obj.series_names():
            for member in document_obj.series_ingest_locations(name):
                uid = str(member.get("uid", ""))
                location = str(member.get("location", ""))
                if not uid or not location or not member.get("ingest"):
                    continue
                if not _is_safe_uid(uid):
                    raise ValueError(
                        f'Refusing to ingest series member of "{name}" (index '
                        f'{member.get("index")}): uid {uid!r} is not a plain '
                        f"filename. See DID-python issue #58."
                    )
                is_remote = self._is_remote_location(
                    location, member.get("location_type")
                )
                if is_remote and custom_file_handler is None:
                    warnings.warn(
                        f'Series member "{name}_{member.get("index")}" '
                        f'location "{location}" is marked for ingestion but '
                        f"is not a local path, and no custom_file_handler "
                        f"was supplied. The member is skipped.",
                        stacklevel=2,
                    )
                    continue

                os.makedirs(file_dir, exist_ok=True)
                dest_path = os.path.join(file_dir, uid)
                if os.path.exists(dest_path):
                    # Adding the same doc again: the copy is already in place.
                    continue

                source_path = location if is_remote else self._resolve_local(location)
                try:
                    if is_remote:
                        handler_context = {
                            "documentId": document_obj.id(),
                            "filename": f"{name}_{member.get('index')}",
                            "seriesName": name,
                            "uid": uid,
                            # "add", not "ingest": mode is a string the
                            # CALLER's handler switches on, so it is part of
                            # the cross-language contract rather than an
                            # internal name. MATLAB sends 'add' from both of
                            # do_add_doc's loops (sqlitedb.m:567, :664) and
                            # 'open' from the read path, which Python already
                            # matches. See DID-python#71.
                            "mode": "add",
                        }
                        self._dispatch_custom_file_handler(
                            custom_file_handler,
                            dest_path,
                            source_path,
                            handler_context,
                        )
                    else:
                        shutil.copyfile(source_path, dest_path)
                except Exception as error:  # noqa: BLE001 - warn and continue
                    # Whatever was half-written is not this member's bytes,
                    # and the short circuit above would accept it as such on
                    # the next add. Clear it before reporting.
                    with contextlib.suppress(OSError):
                        os.remove(dest_path)
                    warnings.warn(
                        f'Failed to ingest series member "{name}_'
                        f'{member.get("index")}" from "{location}": {error}',
                        stacklevel=2,
                    )
                    continue

                if not os.path.isfile(dest_path):
                    # MATLAB tests isfile(destPath) after the handler returns
                    # and warns when nothing was produced (sqlitedb.m:669).
                    # A handler that returns cleanly having written nothing is
                    # otherwise indistinguishable from one that succeeded, and
                    # the member is simply absent with nobody told. See
                    # DID-python#71.
                    warnings.warn(
                        f'Failed to ingest series member "{name}_'
                        f'{member.get("index")}" from "{location}": '
                        f"{'custom_file_handler did not produce a file' if is_remote else 'no file was produced'} "
                        f'at "{dest_path}"',
                        stacklevel=2,
                    )
                    continue

                if member.get("delete_original") and not is_remote:
                    self._delete_after_commit(source_path, "series member")

    def _open_series_member(self, series_locations, custom_file_handler):
        """Open a file series member: local first, then fetch through handler.

        A member has no ``orig_location`` -- that is a per-member record a
        series deliberately does not keep. But the series MANIFEST has a
        location, and a handler that can reach that store can reach a
        sibling object in it given the member's uid. Following
        DID-matlab#188, we hand the handler the manifest's location as
        ``sourcePath`` and the member's uid in the context, plus
        ``seriesName``, so the handler can decide how to fetch it. DID
        composes no URL and learns no scheme.

        Returns a :class:`ReadOnlyFileobj`, or raises
        :class:`FileAccessError`.
        """
        from ..database import (
            FileAccessError,
            _series_manifest_path,
        )
        from ..file import (
            ReadOnlyFileobj,
            SeriesManifestError,
            cached_path_for_uid,
            read_series_manifest_uid,
        )

        doc = series_locations.document_obj
        filename = series_locations.filename
        stem, index = doc.series_member_of(filename)
        additional_roots = self._do_cached_path_roots()

        manifest_path = _series_manifest_path(doc, stem, additional_roots)
        if manifest_path is None and custom_file_handler is not None:
            # The manifest bytes are not on this machine, but a handler was
            # supplied. Offer the manifest's own file_info location(s) to
            # it (mode='open', ctx.uid=<manifestUid>, ctx.seriesName='') --
            # the same call shape do_open_doc uses for a non-'file' file
            # -- so a document that arrived from a store which keeps bytes
            # remote (SyncFiles=false through NDI Cloud, say) can still be
            # resolved. Success caches at filecachepath/<manifestUid>, so
            # the next cached_path_for_uid is a hit and a second member
            # open on the same series pays no network for the manifest
            # again. See VH-Lab/DID-matlab#201 and VH-Lab/DID-python#87.
            manifest_path = self._fetch_series_manifest_bytes(
                doc, stem, custom_file_handler
            )
        if manifest_path is None:
            # We cannot open the manifest, so we cannot know what member the
            # series records. This is the same "not here" cached_path_for_file
            # reports; open_doc says which series is missing.
            raise FileAccessError(
                "DID:SQLITEDB:FileSeries:ManifestNotLocal",
                f'Cannot open series member "{filename}" of document '
                f'"{doc.id()}": the series "{stem}" manifest is not on this '
                f"machine.",
            )

        if index < 1:
            # Out of bounds, not damaged. Slots are one-based, so slot 0 is
            # as absent as a slot past the end -- and read_series_manifest_uid
            # raises ValueError for it, which the arm below would report as
            # ManifestUnreadable, sending the caller after a corruption that
            # is not there. MATLAB guards the same way before touching the
            # manifest (sqlitedb.m seriesMemberPath: index < 1 || index ~=
            # round(index)). See DID-python#80.
            raise FileAccessError(
                "DID:SQLITEDB:FileSeries:NoSuchMember",
                f'No such member "{filename}" in series "{stem}" of document '
                f'"{doc.id()}": member indices are one-based.',
            )

        try:
            member_uid, _count = read_series_manifest_uid(manifest_path, index)
        except (OSError, SeriesManifestError, ValueError) as error:
            raise FileAccessError(
                "DID:SQLITEDB:FileSeries:ManifestUnreadable",
                f'Cannot read the manifest for series "{stem}" of document '
                f'"{doc.id()}": {error}',
            ) from error

        if not member_uid:
            raise FileAccessError(
                "DID:SQLITEDB:FileSeries:NoSuchMember",
                f'No such member "{filename}" in series "{stem}" of document '
                f'"{doc.id()}": the manifest records no uid at slot {index}.',
            )

        # Local first: same two roots, same order, that every other file uses.
        local_path = cached_path_for_uid(member_uid, additional_roots=additional_roots)
        if local_path:
            return ReadOnlyFileobj(local_path)

        # Not here; try to fetch via the manifest's location. Series members
        # get exactly one shot per open_doc.
        if custom_file_handler is None:
            raise FileAccessError(
                "DID:SQLITEDB:FileSeries:MemberNotHere",
                f'Series member "{filename}" of series "{stem}" (uid '
                f'"{member_uid}") is not on this machine and no '
                f"custom_file_handler was supplied.",
            )

        # A two-argument handler is told nothing but the manifest's location,
        # so it has no way to learn which member it is being asked for:
        # anything it produced would be the wrong bytes under this uid. It is
        # not asked at all -- the member is simply not here, as when no
        # handler was supplied. Mirrors MATLAB's fetchSeriesMemberBytes gate.
        if not self._handler_takes_context(custom_file_handler):
            raise FileAccessError(
                "DID:SQLITEDB:FileSeries:MemberNotHere",
                f'Series member "{filename}" of series "{stem}" (uid '
                f'"{member_uid}") is not on this machine, and the '
                f"custom_file_handler takes only (dest_path, source_path), so "
                f"it cannot be told which member to fetch. A handler that "
                f"accepts a third, context argument is given the member's uid.",
            )

        # Offer every location of the manifest to the handler, remote first
        # so a handler that can answer from the manifest's store is not
        # given a local copy it can only re-download from itself.
        manifest_locations = self._manifest_locations_for_handler(doc, stem)
        if not manifest_locations:
            raise FileAccessError(
                "DID:SQLITEDB:FileSeries:MemberNotHere",
                f'Series member "{filename}" of series "{stem}" (uid '
                f'"{member_uid}") is not on this machine and the manifest '
                f"records no location a handler could reach.",
            )

        # DID-matlab#191: guard against a handler that resolves the manifest
        # location instead of the member uid -- it would return the
        # manifest's bytes, cache them under the member's uid, and silently
        # corrupt every later member read.
        from ..common import PathConstants

        try:
            manifest_size = os.path.getsize(manifest_path)
        except OSError:
            manifest_size = None
        temp_dir = PathConstants().temppath
        failures = []
        for src_location in manifest_locations:
            fetched_path, error = self._fetch_remote_to_cache(
                member_uid,
                src_location,
                temp_dir,
                custom_file_handler,
                context={
                    "documentId": doc.id(),
                    "filename": filename,
                    "seriesName": stem,
                    "uid": member_uid,
                    "mode": "open",
                },
            )
            if fetched_path is None:
                failures.append(f"{src_location}: {error}")
                continue

            try:
                fetched_size = os.path.getsize(fetched_path)
            except OSError as error:
                # A file that was just fetched and cannot be measured is not
                # a file we can hand back. Treating this as "comparison
                # unavailable, accept it" is what turned a stale cache entry
                # into a silent empty read. See DID-python#73.
                self._discard_fetched(fetched_path, member_uid)
                failures.append(f"{src_location}: fetched file is unreadable: {error}")
                continue

            if manifest_size is not None and fetched_size == manifest_size:
                try:
                    with (
                        open(fetched_path, "rb") as f_fetched,
                        open(manifest_path, "rb") as f_manifest,
                    ):
                        same_bytes = f_fetched.read() == f_manifest.read()
                except OSError:
                    same_bytes = False  # cannot compare; the size alone is not proof
                if same_bytes:
                    self._discard_fetched(fetched_path, member_uid)
                    failures.append(
                        f"{src_location}: handler returned the "
                        f"manifest's own bytes (see DID-matlab #191)"
                    )
                    continue

            return ReadOnlyFileobj(fetched_path)

        raise FileAccessError(
            (
                "DID:SQLITEDB:FileSeries:HandlerReturnedManifest"
                if failures and "DID-matlab #191" in failures[-1]
                else "DID:SQLITEDB:FileSeries:MemberFetchFailed"
            ),
            f'Cannot fetch series member "{filename}" of "{stem}" in document '
            f'"{doc.id()}": ' + "; ".join(failures),
        )

    def _discard_fetched(self, fetched_path, uid):
        """Throw away a fetch that turned out not to be the file we wanted.

        ``_fetch_remote_to_cache`` does not hand back the scratch download:
        on success it MOVES the file into the file cache under ``uid`` and
        returns the path inside the cache. So removing that path with a bare
        ``os.remove`` takes the file and leaves the cache's index entry
        behind, and ``cache.is_file(uid)`` then answers True forever for a
        file that is not there -- every later fetch of that uid short
        circuits on the index and returns the missing path, which
        ``Fileobj.fopen()`` opens as b"" without raising.

        That is the corruption the DID-matlab#191 guard exists to prevent,
        reached by the guard itself. So the removal goes THROUGH the cache
        when the path is the cache's copy, and around it otherwise (no
        cache, or a uid the cache would not index). See DID-python#73.
        """
        cache = self._file_cache()
        if cache is not None and uid:
            try:
                if cache.is_file(uid) and os.path.abspath(
                    cache.full_path(uid)
                ) == os.path.abspath(fetched_path):
                    cache.remove_file(uid)
                    return
            except (OSError, ValueError, struct.error):
                pass  # cache unusable; fall through to the plain remove
        with contextlib.suppress(OSError):
            os.remove(fetched_path)

    def _fetch_series_manifest_bytes(
        self, document_obj, series_name, custom_file_handler
    ):
        """Fetch a series' manifest via ``custom_file_handler``.

        ONE LEVEL UP from :meth:`_fetch_remote_to_cache` for a member: the
        manifest is an ordinary file of the document, so its ``file_info``
        entry names its location(s) directly -- no SQL, no recursion into
        ``open_doc``. Every non-``'file'`` location is offered to the
        handler with the manifest's own uid in the context and
        ``mode='open'``, in the same call shape ``open_doc`` uses for a
        non-``'file'`` file_info location on the read path. Success puts
        the bytes at ``filecachepath/<manifestUid>`` so the next
        :func:`cached_path_for_uid` is a hit, whether the next call is
        this member, another member, or a later session.

        The no-network primitive :func:`did.database._series_manifest_path`
        must stay callable from any thread and any process, so teaching it
        to fetch would break that. The lazy-fetch path lives here on the
        sqlite implementation, where the handler already lives, and the
        local-first fast path in :meth:`_open_series_member` keeps the
        second open of a series one :func:`cached_path_for_uid` hit.

        Returns the local path on success, or ``None`` on any failure --
        the caller reports the miss in its own words. See
        VH-Lab/DID-matlab#201 and VH-Lab/DID-python#87.
        """
        from ..common import PathConstants

        if custom_file_handler is None or document_obj is None:
            return None

        is_in, info, _ = document_obj.is_in_file_list(series_name)
        if not is_in or not isinstance(info, dict):
            return None
        locations = info.get("locations")
        if isinstance(locations, dict):
            locations = [locations]
        if not isinstance(locations, list):
            return None

        temp_dir = PathConstants().temppath
        for entry in locations:
            if not isinstance(entry, dict):
                continue
            location = entry.get("location")
            uid = entry.get("uid")
            location_type = entry.get("location_type")
            if not location or not uid:
                continue
            # Only non-'file' locations are offered. cached_path_for_uid has
            # already checked filecachepath/<uid> and FileDir/<uid>, and a
            # manifest orig_location that points elsewhere is not a resolution
            # rule DID promises.
            if not self._is_remote_location(location, location_type):
                continue

            fetched_path, _error = self._fetch_remote_to_cache(
                uid,
                location,
                temp_dir,
                custom_file_handler,
                context={
                    "documentId": document_obj.id(),
                    "filename": str(series_name),
                    "seriesName": "",
                    "uid": uid,
                    "mode": "open",
                },
            )
            if fetched_path is not None and os.path.isfile(fetched_path):
                return fetched_path

        return None

    def _manifest_locations_for_handler(self, document_obj, stem):
        """Locations the handler should be offered to fetch a series member.

        Remote first, then any local locations recorded for the manifest.
        Following DID-matlab#191, EVERY location is offered (including a
        local one for the manifest itself); the guard in
        :meth:`_open_series_member` refuses a handler that returns the
        manifest's own bytes, so an offered local location is harmless
        rather than incorrect.
        """
        # Get every location for the manifest file.
        is_in, info, _ = document_obj.is_in_file_list(stem)
        if not is_in or not isinstance(info, dict):
            return []
        locations = info.get("locations")
        if isinstance(locations, dict):
            locations = [locations]
        if not isinstance(locations, list):
            return []

        remote_locations = []
        local_locations = []
        for entry in locations:
            loc = entry.get("location")
            if not loc:
                continue
            if self._is_remote_location(loc, entry.get("location_type")):
                remote_locations.append(loc)
            else:
                # Only offer a local path that actually exists; a stale
                # entry would just be a failing round-trip.
                resolved = self._resolve_local(loc)
                if os.path.isfile(resolved):
                    local_locations.append(resolved)
        return remote_locations + local_locations

    def _fetch_remote_to_cache(
        self, uid, location, temp_dir, custom_file_handler, context=None
    ):
        """Fetch ``location`` through ``custom_file_handler``, holding a
        per-uid lock and using a unique scratch name.

        Mirrors DID-matlab issue #185 / #173: the fetch lock is
        ``<temppath>/<uid>-fetch-lock`` with ``checkloops=30``,
        ``throwerror=False``, ``expiration=300``. Two processes fetching the
        same uid serialise here; correctness rests on the unique scratch
        name and the ``add_file`` fallback, so a lock the caller cannot get
        must not fail the read. After the lock, the cache is re-checked:
        the whole point is that the second caller finds the file already
        there instead of re-downloading.

        Returns ``(fetched_path, error)``: on success ``fetched_path`` is
        the path to use, and ``error`` is ``None``. On failure the reverse.

        The handler is called with either ``(dest_path, source_path)`` or
        ``(dest_path, source_path, context)`` depending on its declared
        signature (DID-matlab#186 -- widen customFileHandler with document
        context). A handler that accepts fewer positional args is called
        with two; anything else is called with three.
        """
        import tempfile

        from ..file import checkout_lock_file, release_lock_file

        os.makedirs(temp_dir, exist_ok=True)
        lock_path = os.path.join(temp_dir, f"{uid}-fetch-lock")
        lock_result = checkout_lock_file(
            lock_path, check_loops=30, throw_error=False, expiration=300
        )
        # ``checkout_lock_file`` returns ``(handle, key)``. ``throw_error=False``
        # falls through as ``(None, None)`` if the lock could not be taken;
        # correctness rests on the unique scratch name and the ``add_file``
        # fallback, so proceeding without the lock is safe.
        lock_key = lock_result[1] if lock_result and lock_result[0] else None

        try:
            # After the lock, re-check the cache: the whole point is that
            # the second caller does not re-download.
            cache = self._file_cache()
            if cache is not None and uid:
                try:
                    if cache.is_file(uid):
                        cached_path = cache.full_path(uid)
                        # The index says the cache holds it; the filesystem
                        # is what decides. An entry whose file has gone --
                        # evicted behind our back, cleared by hand, or
                        # removed by a caller that went around the cache --
                        # would otherwise be handed back as a completed
                        # fetch, and a Fileobj over a path that does not
                        # exist reads b"" without raising. Refetch instead.
                        # See DID-python#73.
                        if os.path.isfile(cached_path):
                            return cached_path, None
                        with contextlib.suppress(OSError, ValueError, struct.error):
                            cache.remove_file(uid)
                except (OSError, ValueError, struct.error):
                    pass  # cache unusable; carry on and refetch

                # Any earlier partial at temppath is not ours to reuse.
                # (New downloads use unique names below.)

            # Unique temp name per fetch: <uid>.<random>.part in temppath.
            fd, dest_path = tempfile.mkstemp(
                dir=temp_dir, prefix=f"{uid}.", suffix=".part"
            )
            os.close(fd)
            # Make the destination available to the handler; MATLAB's own
            # scratch also starts non-existent.
            try:
                os.remove(dest_path)
            except OSError:
                pass

            try:
                handler_context = self._handler_context(context)
                self._dispatch_custom_file_handler(
                    custom_file_handler, dest_path, location, handler_context
                )
            except BaseException as error:
                # Delete the partial so nothing is left to be mistaken for a
                # completed download; catch BaseException so KeyboardInterrupt
                # cleans up too -- that is one way a partial gets created.
                with contextlib.suppress(OSError):
                    os.remove(dest_path)
                if isinstance(error, Exception):
                    return None, str(error)
                raise

            if not os.path.isfile(dest_path):
                # Handler returned no file. Clean up and report.
                with contextlib.suppress(OSError):
                    os.remove(dest_path)
                return None, (
                    f'custom_file_handler did not produce a file at "{dest_path}"'
                )

            # Race with another fetch of the same uid: the cache may have
            # gained a copy while we were downloading. Losing the race at
            # add_file is benign -- those are the same bytes, so use them.
            cached = self._add_to_file_cache(dest_path, uid)
            if cached is not None:
                # add_file moved the copy in; remove the partial we wrote.
                with contextlib.suppress(OSError):
                    os.remove(dest_path)
                return cached, None
            # No cache available -- return the temp path itself.
            return dest_path, None
        finally:
            if lock_key is not None:
                with contextlib.suppress(OSError):
                    release_lock_file(lock_path, lock_key)

    @staticmethod
    def _handler_context(context):
        """Return a defensive copy of the handler-context dict.

        DID-matlab#186 widens the customFileHandler signature to receive a
        scalar struct with per-call document context (documentId, filename,
        seriesName, uid, mode). Pass ``None`` when no context is available.
        """
        if context is None:
            return None
        return dict(context)

    @staticmethod
    def _handler_takes_context(handler):
        """Will ``handler`` be given the context?

        True when it declares three or more positional parameters, or takes
        ``*args``. A handler declared with two gets the two-argument call and
        never sees a context. Mirrors MATLAB's
        ``did.implementations.sqlitedb.handlerTakesContext``: the rule
        _dispatch_custom_file_handler dispatches by, named once so a caller
        with nothing useful to say to a two-argument handler -- the series
        member fetch in _open_series_member, whose whole message is in the
        context -- can ask rather than re-derive it.

        A callable whose signature cannot be inspected answers False, as
        MATLAB's does when ``nargin`` fails: the safe default is the old
        contract.
        """
        import inspect

        try:
            signature = inspect.signature(handler)
        except (TypeError, ValueError):
            return False
        # Count positional-capable parameters (POSITIONAL_ONLY /
        # POSITIONAL_OR_KEYWORD / VAR_POSITIONAL).
        positional = 0
        for p in signature.parameters.values():
            if p.kind == inspect.Parameter.VAR_POSITIONAL:
                return True
            if p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                positional += 1
        return positional >= 3

    @staticmethod
    def _dispatch_custom_file_handler(handler, dest_path, source_path, context):
        """Call ``handler`` with two or three arguments per its signature.

        A handler declared with three or more positional inputs (or with
        ``*args``) is called with ``(dest_path, source_path, context)``. A
        handler declared with two inputs is called without context, so the
        older two-argument signature still works. Mirrors MATLAB's
        ``did.implementations.sqlitedb.dispatchCustomFileHandler``; the rule
        itself is :meth:`_handler_takes_context`.
        """
        import inspect

        if context is None:
            handler(dest_path, source_path)
            return
        try:
            inspect.signature(handler)
        except (TypeError, ValueError):
            # Builtin or C-implemented callable: fall back to positional try.
            try:
                handler(dest_path, source_path, context)
                return
            except TypeError:
                handler(dest_path, source_path)
                return

        if SQLiteDB._handler_takes_context(handler):
            handler(dest_path, source_path, context)
        else:
            handler(dest_path, source_path)

    def _add_to_file_cache(self, source_path, uid):
        """Move a freshly retrieved file into the file cache.

        Returns its path inside the cache, or None if it could not be cached
        -- in which case the caller keeps using the file where it landed.
        Failing to cache is never a reason to fail the open: the file was
        retrieved, and the only cost is retrieving it again next time.
        """
        cache = self._file_cache()
        if cache is None or not uid:
            return None
        name = str(uid)
        if len(name) != cache.file_name_characters:
            # The cache's rows are fixed-width, so a uid of another length
            # cannot be indexed. Nothing is lost but the caching.
            return None
        try:
            if cache.is_file(name):
                cache.remove_file(name)
            cache.add_file(source_path, name)
        except (OSError, ValueError, struct.error):
            return None
        return cache.full_path(name)

    @staticmethod
    def _create_deleted_docs_table(cursor):
        """Create the `deleted_docs` table (DID-matlab issue #55)."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deleted_docs (
                doc_id TEXT NOT NULL UNIQUE,
                timestamp REAL,
                PRIMARY KEY(doc_id)
            )
        """)

    @staticmethod
    def _has_deleted_docs_table(cursor):
        """Does this database carry a `deleted_docs` table?

        The table arrived with DID-matlab issue #55, so a database written by
        an earlier DID -- in either language -- does not have one. Its absence
        is never an error: it simply means no document has been permanently
        removed from it yet. MATLAB takes the same view, and neither language
        adds the table to a database merely by reading it.
        """
        cursor.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'deleted_docs'"
        )
        return cursor.fetchone() is not None

    def _is_deleted_doc_id(self, cursor, doc_id):
        """Was a document with this id permanently removed?"""
        if not self._has_deleted_docs_table(cursor):
            return False
        cursor.execute("SELECT doc_id FROM deleted_docs WHERE doc_id = ?", (doc_id,))
        return cursor.fetchone() is not None

    def _record_deleted_doc_id(self, cursor, doc_id):
        """Retire a document id, so that it is never re-used.

        Creates the table on demand rather than at open, so that merely
        reading a database written by an earlier DID never writes to it.
        OR IGNORE: recording an id that is already retired is a no-op, not a
        UNIQUE-constraint failure.
        """
        import time

        if not self._has_deleted_docs_table(cursor):
            self._create_deleted_docs_table(cursor)
        cursor.execute(
            "INSERT OR IGNORE INTO deleted_docs (doc_id, timestamp) VALUES (?, ?)",
            (doc_id, time.time()),
        )

    def _reclaim_unreferenced_doc(self, cursor, doc_idx, doc_id):
        """Erase every trace of a document no branch references any more.

        Called once the last branch_docs row for DOC_IDX is gone, from both
        paths that can orphan a document: _do_remove_doc and
        _do_delete_branch. Deletes the doc_data, files and docs rows and
        retires DOC_ID (DID-matlab issue #55).

        The delete order matters: doc_data and files both carry
        FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx), and with
        PRAGMA foreign_keys = ON the docs delete is refused while any of them
        survives. MATLAB runs the same three deletes in the same order.

        Returns the ingested copies' paths for the caller to unlink AFTER the
        transaction commits. MATLAB deletes them before the rows, but it runs
        no transaction; here, unlinking first would mean a later SQL failure
        rolled the rows back with the files already gone. Deleting them after
        the commit can at worst leave an orphaned file, which is recoverable.
        """
        cursor.execute(
            "SELECT cached_location FROM files WHERE doc_idx = ?", (doc_idx,)
        )
        # Materialize before reusing the cursor for the deletes below.
        cached_paths = [
            row["cached_location"]
            for row in cursor.fetchall()
            if row["cached_location"]
        ]

        cursor.execute("DELETE FROM doc_data WHERE doc_idx = ?", (doc_idx,))
        cursor.execute("DELETE FROM files WHERE doc_idx = ?", (doc_idx,))
        cursor.execute("DELETE FROM docs WHERE doc_idx = ?", (doc_idx,))

        self._record_deleted_doc_id(cursor, doc_id)
        return cached_paths

    @staticmethod
    def _remove_cached_files(paths):
        """Delete ingested copies from disk, ignoring ones already gone.

        A file that is not there is not a failure: the point is that it must
        not be there afterwards. MATLAB suppresses the same case by turning
        off MATLAB:DELETE:FileNotFound around its delete() loop.
        """
        for path in paths:
            try:
                os.remove(path)
            except OSError:
                pass

    @_journaled
    def _do_remove_doc(self, document_id, branch_id, **kwargs):
        cursor = self._journal_cursor()

        # Check if branch exists
        cursor.execute("SELECT 1 FROM branches WHERE branch_id = ?", (branch_id,))
        if not cursor.fetchone():
            raise ValueError(f"Branch '{branch_id}' does not exist.")

        # Get doc_idx from doc_id
        cursor.execute("SELECT doc_idx FROM docs WHERE doc_id = ?", (document_id,))
        row = cursor.fetchone()

        if row:
            doc_idx = row["doc_idx"]
            cached_paths = []
            try:
                # Remove from branch_docs
                cursor.execute(
                    "DELETE FROM branch_docs WHERE branch_id = ? AND doc_idx = ?",
                    (branch_id, doc_idx),
                )

                # If no branch references this document any more, it is gone
                # for good, so nothing of it should be left behind: its rows,
                # its ingested files and its id all go.
                cursor.execute(
                    "SELECT COUNT(*) FROM branch_docs WHERE doc_idx = ?", (doc_idx,)
                )
                count = cursor.fetchone()[0]
                if count == 0:
                    cached_paths = self._reclaim_unreferenced_doc(
                        cursor, doc_idx, document_id
                    )

                self._commit()
            except Exception:
                # As in _do_add_doc: a failure part-way through must not leave
                # the branch_docs delete sitting in an open transaction for the
                # next commit on this connection to pick up.
                self._rollback()
                raise

            # Only once the rows are committed - see _reclaim_unreferenced_doc.
            self._remove_cached_files(cached_paths)
        else:
            # Handle missing document
            on_missing = kwargs.get("OnMissing", "error").lower()
            if on_missing == "warn":
                print(f"Warning: Document id '{document_id}' not found for removal.")
            elif on_missing != "ignore":
                raise ValueError(f"Document id '{document_id}' not found for removal.")

    @_journaled
    def _do_delete_branch(self, branch_id):
        """Delete a branch, reclaiming any document it was the last to hold.

        Deleting a branch is the second way a document comes to be referenced
        by no branch at all, so it owes the same clean-up _do_remove_doc does
        (DID-matlab issue #55). The guard is the same test - no surviving
        branch_docs row anywhere - so a document that any other branch still
        holds is left completely alone. Deleting documents from one branch
        does not delete them from other branches.
        """
        cursor = self._journal_cursor()
        cached_paths = []
        try:
            # Note which documents this branch holds BEFORE dropping its rows:
            # afterwards nothing leads from the branch back to them.
            cursor.execute(
                "SELECT docs.doc_idx AS doc_idx, docs.doc_id AS doc_id "
                "FROM docs, branch_docs "
                "WHERE docs.doc_idx = branch_docs.doc_idx "
                "AND branch_docs.branch_id = ?",
                (branch_id,),
            )
            held = [(row["doc_idx"], row["doc_id"]) for row in cursor.fetchall()]

            cursor.execute("DELETE FROM branch_docs WHERE branch_id = ?", (branch_id,))
            cursor.execute("DELETE FROM branches WHERE branch_id = ?", (branch_id,))

            for doc_idx, doc_id in held:
                cursor.execute(
                    "SELECT COUNT(*) FROM branch_docs WHERE doc_idx = ?", (doc_idx,)
                )
                if cursor.fetchone()[0] == 0:
                    cached_paths.extend(
                        self._reclaim_unreferenced_doc(cursor, doc_idx, doc_id)
                    )

            self._commit()
        except Exception:
            self._rollback()
            raise

        # Only once the rows are committed - see _reclaim_unreferenced_doc.
        self._remove_cached_files(cached_paths)

    def _do_get_sub_branches(self, branch_id):
        rows = self.do_run_sql_query(
            "SELECT branch_id FROM branches WHERE parent_id = ?", (branch_id,)
        )
        return [row["branch_id"] for row in rows]

    def _do_get_branch_parent(self, branch_id):
        row = self.do_run_sql_query(
            "SELECT parent_id FROM branches WHERE branch_id = ?", (branch_id,)
        )
        if row:
            return row[0]["parent_id"]
        return None
