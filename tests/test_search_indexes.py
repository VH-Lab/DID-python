"""The searches NDI makes most must be fast in SQLiteDB.

NDI searches a session for documents by id and by class::

    Query("base.id", "exact_string", ID) & Query("base.session_id", "exact_string", S)
    Query("", "isa", "daqsystem")        & Query("base.session_id", "exact_string", S)

The terms of an AND used to be searched separately over the whole branch and
their results intersected, so the session term returned every document of the
session; and 'isa' ran a regexp on the superclass list of every document. Now
base.id terms are searched first, base.session_id terms after the others and
whole-value tests (regexp, brute force) last; each later term is checked
against the documents already found only; and 'isa' runs its regexp on the
distinct superclass lists only -- through the doc_data indexes doc_data_doc_idx
and doc_data_field_value. See SQLiteDB._search_doc_ids. Mirrors DID-matlab's
did.unittest.TestSearchIndexes.

Besides the results, these tests check the number of rows each search query
returned: a lookup that returns a row per document of the session is the slow
path however quickly it happens to run on a small test database.
"""

import contextlib
import io
import os
import sqlite3
import statistics
import tempfile
import time
import unittest
import uuid

from did.document import Document
from did.implementations.sqlitedb import _MAX_SEARCH_CANDIDATES, SQLiteDB
from did.query import Query

NUM_SESSION_DOCS = 1500  # documents in the main session (demoA)
NUM_OTHER_DOCS = 100  # documents in a second session (demoB)
SEARCH_INDEXES = ("doc_data_doc_idx", "doc_data_field_value")


class _QueryLogDB(SQLiteDB):
    """A SQLiteDB that records each search query and how many rows it returned."""

    def __init__(self, filename):
        self.query_log = []
        self.loaded_doc_ids = []
        super().__init__(filename)

    def do_run_sql_query(self, query_str, params=()):
        rows = super().do_run_sql_query(query_str, params)
        if query_str.startswith("SELECT DISTINCT docs.doc_id"):
            self.query_log.append((query_str, len(rows)))
        return rows

    def get_docs(self, document_ids, *args, **kwargs):
        ids = document_ids if isinstance(document_ids, list) else [document_ids]
        self.loaded_doc_ids.extend(ids)
        return super().get_docs(document_ids, *args, **kwargs)


def _doc(class_name, superclasses, session_id, value):
    props = {
        "base": {
            "id": uuid.uuid4().hex,
            "session_id": session_id,
            "name": "",
            "datestamp": "2026-10-24T00:00:00.000Z",
        },
        "document_class": {
            "class_name": class_name,
            "superclasses": [{"class_name": sc} for sc in superclasses],
        },
        "demoA": {"value": value},
    }
    if class_name == "demoB":
        props["demoB"] = {"value": value}
    return Document(props)


def _id_query(doc_id):
    return Query("base.id", "exact_string", doc_id)


def _session_query(session_id):
    return Query("base.session_id", "exact_string", session_id)


def _or_of_ids(ids):
    """A balanced OR of base.id queries (shallow, whatever the count)."""
    if len(ids) == 1:
        return _id_query(ids[0])
    half = len(ids) // 2
    return _or_of_ids(ids[:half]) | _or_of_ids(ids[half:])


class TestSearchIndexes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.db = _QueryLogDB(os.path.join(cls.tmpdir.name, "search_indexes.sqlite"))
        cls.db.add_branch("a")
        cls.session = uuid.uuid4().hex
        cls.other_session = uuid.uuid4().hex
        docs = [
            _doc("demoA", ["base"], cls.session, i) for i in range(NUM_SESSION_DOCS)
        ]
        other = [
            _doc("demoB", ["base", "demoA"], cls.other_session, i)
            for i in range(NUM_OTHER_DOCS)
        ]
        cls.db.add_docs(docs + other, branch_id="a", validate=False)
        cls.ids = [d.id() for d in docs]
        cls.other_ids = [d.id() for d in other]

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        cls.tmpdir.cleanup()

    def tearDown(self):
        self.db._search_indexes_ready = True

    def search_logged(self, query):
        """Search; return (ids, most rows any search query returned, query count)."""
        self.db.query_log = []
        found = self.db.search(query, branch_id="a")
        rows = [n for _, n in self.db.query_log]
        return found, max(rows, default=0), len(rows)

    def search_both_ways(self, query):
        """Search with the indexes, then as if the database had none."""
        self.db._search_indexes_ready = True
        fast = self.db.search(query, branch_id="a")
        self.db._search_indexes_ready = False
        try:
            full = self.db.search(query, branch_id="a")
        finally:
            self.db._search_indexes_ready = True
        return sorted(fast), sorted(full)

    @staticmethod
    def index_names(db):
        rows = db.dbid.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
        return {row[0] for row in rows}

    # --- the indexes -------------------------------------------------------

    def test_new_database_has_search_indexes(self):
        names = self.index_names(self.db)
        for name in SEARCH_INDEXES:
            self.assertIn(name, names)
        self.assertTrue(self.db._search_indexes_ready)

    def test_new_database_is_created_quietly(self):
        # A new database's tables are empty, so its indexes build instantly;
        # announcing them would print three lines for every database created.
        with tempfile.TemporaryDirectory() as tmp:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                db = SQLiteDB(os.path.join(tmp, "new.sqlite"))
            try:
                self.assertTrue(db._search_indexes_ready)
                self.assertNotIn("search index", stderr.getvalue())
            finally:
                db._close_db()

    def test_indexes_added_when_older_database_opened(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "older.sqlite")
            older = SQLiteDB(path)
            older.add_branch("a")
            doc = _doc("demoA", ["base"], self.session, 1)
            older.add_docs([doc], branch_id="a", validate=False)
            for name in SEARCH_INDEXES:
                older.dbid.execute(f'DROP INDEX "{name}"')
            older.dbid.commit()
            older._close_db()

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                reopened = SQLiteDB(path)
            try:
                names = self.index_names(reopened)
                for name in SEARCH_INDEXES:
                    self.assertIn(name, names, f"opening must add {name}")
                    # building an index on a large database can take minutes,
                    # so each one is announced and timed, not built silently
                    self.assertIn(f"adding search index {name}", stderr.getvalue())
                    self.assertIn(f"added search index {name} in ", stderr.getvalue())
                self.assertTrue(reopened._search_indexes_ready)
                q = _id_query(doc.id()) & _session_query(self.session)
                self.assertEqual(reopened.search(q, branch_id="a"), [doc.id()])
            finally:
                reopened._close_db()

            # ...and only once: a database that has them opens quietly
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                again = SQLiteDB(path)
            again._close_db()
            self.assertNotIn("search index", stderr.getvalue())

    def test_read_only_database_without_indexes_still_searches(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "readonly.sqlite")
            older = SQLiteDB(path)
            older.add_branch("a")
            doc = _doc("demoB", ["base", "demoA"], self.session, 1)
            older.add_docs([doc], branch_id="a", validate=False)
            for name in SEARCH_INDEXES:
                older.dbid.execute(f'DROP INDEX "{name}"')
            older.dbid.commit()
            older._close_db()

            db = SQLiteDB.__new__(SQLiteDB)
            # Open through a read-only URI, so creating an index must fail
            real_connect = sqlite3.connect

            def read_only_connect(_filename, **kwargs):
                return real_connect(f"file:{path}?mode=ro", uri=True, **kwargs)

            sqlite3.connect = read_only_connect
            try:
                SQLiteDB.__init__(db, path)
            finally:
                sqlite3.connect = real_connect
            try:
                self.assertFalse(db._search_indexes_ready)
                q = Query("", "isa", "demoA") & _session_query(self.session)
                self.assertEqual(db.search(q, branch_id="a"), [doc.id()])
            finally:
                db._close_db()

    # --- base.id lookups ---------------------------------------------------

    def test_id_lookup_alone(self):
        doc_id = self.ids[777]
        found, max_rows, _ = self.search_logged(_id_query(doc_id))
        self.assertEqual(found, [doc_id])
        self.assertLessEqual(max_rows, 1)

    def test_id_and_session_lookup_is_fast(self):
        elapsed = []
        for k in (0, 1, 499, 999, NUM_SESSION_DOCS - 1):
            doc_id = self.ids[k]
            q = _id_query(doc_id) & _session_query(self.session)
            t0 = time.perf_counter()
            found, max_rows, _ = self.search_logged(q)
            elapsed.append(time.perf_counter() - t0)
            self.assertEqual(found, [doc_id])
            self.assertLessEqual(
                max_rows,
                1,
                "the session term must be checked against the one document "
                "found by id, not return every document of the session",
            )
        # The order of the terms must not matter
        q = _session_query(self.session) & _id_query(self.ids[42])
        found, max_rows, _ = self.search_logged(q)
        self.assertEqual(found, [self.ids[42]])
        self.assertLessEqual(max_rows, 1)
        self.assertLess(statistics.median(elapsed), 0.25)

    def test_id_lookup_in_wrong_session_finds_nothing(self):
        q = _id_query(self.ids[3]) & _session_query(self.other_session)
        found, max_rows, _ = self.search_logged(q)
        self.assertEqual(found, [])
        self.assertLessEqual(max_rows, 1)

    def test_unknown_id_skips_the_other_terms(self):
        q = _id_query("no_such_id_0123456789") & _session_query(self.session)
        found, max_rows, num_queries = self.search_logged(q)
        self.assertEqual(found, [])
        self.assertEqual(max_rows, 0)
        self.assertEqual(num_queries, 1, "the session term need not be searched")

    def test_or_of_ids_returns_all_matches_in_one_query(self):
        wanted = [self.ids[k] for k in (5, 50, 500, 1234)] + [self.other_ids[7]]
        q = _id_query(wanted[0])
        for doc_id in wanted[1:]:
            q = q | _id_query(doc_id)  # nested left to right, as a loop builds it
        found, max_rows, num_queries = self.search_logged(q)
        self.assertEqual(sorted(found), sorted(wanted))
        self.assertLessEqual(max_rows, len(wanted))
        self.assertEqual(num_queries, 1)

        q2 = q | _id_query("no_such_id_0123456789")
        self.assertEqual(sorted(self.db.search(q2, branch_id="a")), sorted(wanted))

        found, max_rows, _ = self.search_logged(q & _session_query(self.session))
        self.assertEqual(sorted(found), sorted(wanted[:-1]))
        self.assertLessEqual(max_rows, len(wanted))

    def test_or_of_more_ids_than_one_query_lists(self):
        wanted = self.ids[:1200] + self.other_ids[:10]
        self.assertGreater(len(wanted), _MAX_SEARCH_CANDIDATES)
        q = _or_of_ids(wanted)
        self.assertEqual(sorted(self.db.search(q, branch_id="a")), sorted(wanted))
        q = q & _session_query(self.session)
        self.assertEqual(
            sorted(self.db.search(q, branch_id="a")), sorted(self.ids[:1200])
        )

    # --- isa ----------------------------------------------------------------

    def test_isa_and_session_checks_only_the_class_documents(self):
        isa = Query("", "isa", "demoB")
        session = _session_query(self.other_session)
        for q in (isa & session, session & isa):
            found, max_rows, _ = self.search_logged(q)
            self.assertEqual(sorted(found), sorted(self.other_ids))
            self.assertLessEqual(
                max_rows,
                NUM_OTHER_DOCS,
                "the session term must be searched last, over the isa matches only",
            )
        found, max_rows, _ = self.search_logged(isa & _session_query(self.session))
        self.assertEqual(found, [])
        self.assertLessEqual(max_rows, NUM_OTHER_DOCS)

    def test_isa_matches_with_and_without_indexes(self):
        for class_name in ("demoA", "demoB", "base", "demo", "nosuchclass"):
            for op in ("isa", "~isa"):
                fast, full = self.search_both_ways(Query("", op, class_name))
                self.assertEqual(fast, full, f"{op} {class_name}")
        self.assertEqual(
            len(self.db.search(Query("", "isa", "demoA"), branch_id="a")),
            NUM_SESSION_DOCS + NUM_OTHER_DOCS,
            "every demoB document is also a demoA (its superclass)",
        )
        self.assertEqual(self.db.search(Query("", "isa", "demo"), branch_id="a"), [])

    # --- terms that test every value ----------------------------------------

    def test_regexp_is_checked_last(self):
        regexp = Query("base.datestamp", "regexp", "^[0-9]")
        isa = Query("", "isa", "demoB")
        session = _session_query(self.other_session)
        for q in (
            regexp & isa & session,
            session & regexp & isa,
            isa & session & regexp,
        ):
            found, max_rows, _ = self.search_logged(q)
            self.assertEqual(sorted(found), sorted(self.other_ids))
            self.assertLessEqual(max_rows, NUM_OTHER_DOCS)

    def test_brute_force_term_loads_only_the_candidates(self):
        # hasmember has no SQL form, so it loads and tests documents
        doc_id = self.ids[10]
        q = Query("base.id", "hasmember", doc_id) & _id_query(doc_id)
        self.db.loaded_doc_ids = []
        self.assertEqual(
            self.db.search(q, branch_id="a"), self.db.search(q, branch_id="a")
        )
        self.db.loaded_doc_ids = []
        self.db.search(q, branch_id="a")
        self.assertLessEqual(
            len(self.db.loaded_doc_ids),
            1,
            "a brute-force term after an id lookup must load that document only",
        )

    # --- agreement ----------------------------------------------------------

    def test_restricted_search_matches_full_search(self):
        doc_id, other_id = self.ids[10], self.other_ids[10]
        id_or = _id_query(doc_id) | _id_query(other_id)
        terms = [
            _session_query(self.session),
            Query("base.session_id", "~exact_string", self.session),
            Query("", "isa", "demoB"),
            Query("", "~isa", "demoB"),
            Query("demoB", "hasfield"),
            Query("demoA.value", "exact_number", 10),
            Query("demoA.value", "greaterthan", 5),
            Query("base.id", "contains_string", doc_id[:8]),
            Query("base.datestamp", "regexp", "^[0-9]"),
            Query("demoA.value", "exact_number", 10) | Query("", "isa", "demoB"),
        ]
        for k, term in enumerate(terms):
            for lookup in (_id_query(doc_id), id_or):
                fast, full = self.search_both_ways(lookup & term)
                self.assertEqual(fast, full, f"term {k}")

    def test_searches_without_id_still_work(self):
        q = _session_query(self.other_session) & Query("", "isa", "demoB")
        self.assertEqual(
            sorted(self.db.search(q, branch_id="a")), sorted(self.other_ids)
        )
        q = _session_query(self.session) & Query("demoA.value", "lessthaneq", 2)
        self.assertEqual(len(self.db.search(q, branch_id="a")), 3)


if __name__ == "__main__":
    unittest.main()
