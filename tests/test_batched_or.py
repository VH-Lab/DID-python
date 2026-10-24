"""ORs are searched in batches of SQL terms, with the same results (issue #104).

SQLiteDB._search_or_operands searches the operands of an OR that are single
SQL terms up to ``_or_batch_terms()`` at a time in one SQL query -- SELECTs
combined with UNION over a whole branch, one WHERE with the conditions ORed
together when restricted to candidate documents -- instead of one query per
operand. These tests compare every search with the same search made one query
per operand (``_or_batch_terms() == 1``, the path used before batching), for
every operation the SQL builder supports, plus the negated and brute-forced
operations that stay on that path, in mixed ORs and inside ANDs; and check that
the batched search runs fewer queries. Mirrors DID-matlab's
did.unittest.TestBatchedOr.
"""

import os
import sqlite3
import tempfile
import unittest
import uuid

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query

NUM_A = 60  # demoA documents
NUM_B = 40  # demoB documents (also demoA)
NUM_C = 30  # demoC documents, depending on demoA and demoB documents


class _BatchDB(SQLiteDB):
    """A SQLiteDB that counts search queries and can change the OR batch size."""

    def __init__(self, filename):
        self.query_log = []
        self.or_batch_size = None
        super().__init__(filename)

    def do_run_sql_query(self, query_str, params=()):
        rows = super().do_run_sql_query(query_str, params)
        if query_str.startswith("SELECT DISTINCT docs.doc_id"):
            self.query_log.append(query_str)
        return rows

    def _or_batch_terms(self):
        if self.or_batch_size is None:
            return super()._or_batch_terms()
        return self.or_batch_size


def _doc(class_name, superclasses, name, values, depends_on=()):
    props = {
        "base": {
            "id": uuid.uuid4().hex,
            "session_id": "s1",
            "name": name,
            "datestamp": "2026-10-24T00:00:00.000Z",
        },
        "document_class": {
            "class_name": class_name,
            "superclasses": [{"class_name": sc} for sc in superclasses],
        },
        "depends_on": [{"name": n, "value": v} for n, v in depends_on],
    }
    props.update(values)
    return Document(props)


def _negated(query):
    s = query.to_search_structure()
    s = s[0] if isinstance(s, list) else s
    return Query(s["field"], "~" + s["operation"], s["param1"], s["param2"])


def _or_of(queries):
    q = queries[0]
    for other in queries[1:]:
        q = q | other
    return q


class TestBatchedOr(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.db = _BatchDB(os.path.join(cls.tmpdir.name, "batched_or.sqlite"))
        cls.db.add_branch("a")
        docs_a = [
            _doc("demoA", ["base"], f"n{i}", {"demoA": {"value": i}})
            for i in range(1, NUM_A + 1)
        ]
        docs_b = [
            _doc(
                "demoB",
                ["base", "demoA"],
                f"b{i}",
                {"demoA": {"value": i}, "demoB": {"value": 100 + i}},
            )
            for i in range(1, NUM_B + 1)
        ]
        cls.ids_a = [d.id() for d in docs_a]
        cls.ids_b = [d.id() for d in docs_b]
        docs_c = [
            _doc(
                "demoC",
                ["base"],
                f"c{i}",
                {"demoC": {"value": 200 + i}},
                depends_on=[
                    ("item1", cls.ids_a[i - 1]),
                    ("item2", cls.ids_b[(i - 1) % NUM_B]),
                ],
            )
            for i in range(1, NUM_C + 1)
        ]
        cls.ids_c = [d.id() for d in docs_c]
        cls.db.add_docs(docs_a + docs_b + docs_c, branch_id="a", validate=False)

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        cls.tmpdir.cleanup()

    def tearDown(self):
        self.db.or_batch_size = None
        self.db._search_indexes_ready = True

    def sql_terms(self):
        """A term for every operation _query_struct_to_sql_str turns into SQL."""
        return [
            Query("base.name", "exact_string", "n7"),
            Query("base.name", "exact_string_anycase", "N8"),
            Query("base.name", "contains_string", "b1"),
            Query("base.name", "regexp", "^c2"),
            Query("demoA.value", "exact_number", 12),
            Query("demoA.value", "lessthan", 3),
            Query("demoA.value", "lessthaneq", 3),
            Query("demoB.value", "greaterthan", 138),
            Query("demoB.value", "greaterthaneq", 138),
            Query("demoB", "hasfield"),
            Query("", "isa", "demoB"),
            Query("", "isa", "demoC"),
            Query("", "depends_on", "item1", self.ids_a[3]),
            Query("", "depends_on", "*", self.ids_b[5]),
        ]

    def per_operand_terms(self):
        """Terms that stay on the one-query-per-operand path."""
        negated = [_negated(q) for q in self.sql_terms() if "hasfield" not in str(q)]
        brute = [
            Query("demoC.value", "hasmember", 205),
            Query("demoA.value", "exact_number", "not a number"),
            Query("", "hasanysubfield_contains_string", "item1", "x"),
        ]
        return negated + brute

    def search(self, query, batch_size=None):
        """Search with at most *batch_size* OR operands per SQL query (None =
        the default); return the sorted ids and the number of search queries."""
        self.db.or_batch_size = batch_size
        self.db.query_log = []
        try:
            found = self.db.search(query, branch_id="a")
        finally:
            self.db.or_batch_size = None
        return sorted(found), len(self.db.query_log)

    def assert_batched_matches_per_operand(self, query, msg):
        per_operand, _ = self.search(query, 1)
        batched, _ = self.search(query)
        self.assertEqual(batched, per_operand, f"{msg}: batched search")
        small, _ = self.search(query, 3)
        self.assertEqual(small, per_operand, f"{msg}: batches of 3")

    def test_every_operation_in_an_or(self):
        for term in self.sql_terms() + self.per_operand_terms():
            with self.subTest(term=str(term.to_search_structure())):
                q = _or_of(
                    [
                        term,
                        Query("base.name", "exact_string", "n3"),
                        Query("", "isa", "demoC"),
                    ]
                )
                self.assert_batched_matches_per_operand(q, "term OR ...")

    def test_all_sql_operations_in_one_or(self):
        q = _or_of(self.sql_terms())
        self.assert_batched_matches_per_operand(q, "an OR of every SQL operation")
        _, num_queries = self.search(q)
        self.assertEqual(
            num_queries, 1, "an OR of single SQL terms must be searched in one query"
        )

    def test_every_operation_in_an_or_inside_an_and(self):
        # 'isa demoB' leaves 40 candidates, so the OR is searched restricted
        # to them, with the conditions ORed in one query
        isa_b = Query("", "isa", "demoB")
        for term in self.sql_terms() + self.per_operand_terms():
            with self.subTest(term=str(term.to_search_structure())):
                or_part = _or_of(
                    [
                        term,
                        Query("base.name", "exact_string", "b3"),
                        Query("demoA.value", "lessthan", 5),
                    ]
                )
                q = isa_b & or_part
                self.assert_batched_matches_per_operand(q, "isa demoB AND (...)")
                # ...and it must match the search over the whole branch
                batched, _ = self.search(q)
                self.db._search_indexes_ready = False
                full, _ = self.search(q, 1)
                self.db._search_indexes_ready = True
                self.assertEqual(batched, full, "restricted vs. whole branch")

        q = isa_b & _or_of(self.sql_terms())
        self.assert_batched_matches_per_operand(q, "isa demoB AND every SQL operation")
        _, num_queries = self.search(q)
        self.assertEqual(
            num_queries, 2, "the isa term and the restricted OR take one query each"
        )

    def test_mixed_or(self):
        # base.id lookups, an AND, a negated OR, negated and brute-forced
        # terms among plain ones
        name_is = lambda n: Query("base.name", "exact_string", n)
        terms = [
            Query("base.id", "exact_string", self.ids_a[19]),
            name_is("n5"),
            Query("", "isa", "demoB") & Query("demoA.value", "lessthan", 4),
            Query("", "isa", "demoC"),
            Query("base.id", "exact_string", self.ids_b[19]),
            Query("demoA.value", "~greaterthan", 2),
            Query("base.id", "exact_string", "no_such_id"),
            Query("demoC.value", "hasmember", 205),
        ]
        q = _or_of(terms)
        not_or = {
            "field": "",
            "operation": "~or",
            "param1": [name_is("n1").to_search_structure()[0]],
            "param2": [name_is("n2").to_search_structure()[0]],
        }
        s = q.to_search_structure()
        mixed = [{"field": "", "operation": "or", "param1": s, "param2": [not_or]}]

        class _Raw:
            def __init__(self, structure):
                self.structure = structure

            def to_search_structure(self):
                return self.structure

        for query in (q, _Raw(mixed)):
            self.assert_batched_matches_per_operand(query, "a mixed OR")
            found, num_queries = self.search(query)
            _, per_operand_queries = self.search(query, 1)
            self.assertLess(num_queries, per_operand_queries)
            self.assertTrue({self.ids_a[19], self.ids_b[19]} <= set(found))
            self.assertTrue(set(self.ids_c) <= set(found))

        q2 = Query("base.name", "regexp", "^[nbc][0-9]$") & q
        self.assert_batched_matches_per_operand(q2, "a mixed OR inside an AND")

    def test_long_or_is_searched_in_batches(self):
        # 450 names: 3 queries of at most 200 terms, not 450
        names = [f"n{i}" for i in range(1, NUM_A + 1)] + [
            f"missing{i}" for i in range(390)
        ]
        q = _or_of([Query("base.name", "exact_string", n) for n in names])
        batched, num_queries = self.search(q)
        per_operand, per_operand_queries = self.search(q, 1)
        self.assertEqual(batched, per_operand)
        self.assertEqual(batched, sorted(self.ids_a))
        self.assertEqual(num_queries, 3)
        self.assertEqual(per_operand_queries, 450)

        # ...restricted to candidates: the isa query, then 3 batches
        q2 = Query("", "isa", "demoA") & q
        found, num_queries = self.search(q2)
        self.assertEqual(found, sorted(self.ids_a))
        self.assertEqual(num_queries, 4)
        self.assertEqual(found, self.search(q2, 1)[0])

    def test_without_search_indexes(self):
        # No candidates and no batched id lookups: the batch is a UNION over
        # the whole branch
        self.db._search_indexes_ready = False
        q = _or_of(self.sql_terms() + [Query("base.id", "exact_string", self.ids_b[1])])
        self.assert_batched_matches_per_operand(q, "an OR without the search indexes")
        q2 = Query("", "isa", "demoB") & q
        self.assert_batched_matches_per_operand(
            q2, "an OR in an AND without the search indexes"
        )

    def test_failing_batch_falls_back_to_per_operand(self):
        # A batch whose SQL fails is searched one operand at a time
        q = _or_of(self.sql_terms())
        expected, _ = self.search(q, 1)
        original = SQLiteDB.do_run_sql_query
        calls = []

        def failing(db, query_str, params=()):
            if " UNION " in query_str and not calls:
                calls.append(query_str)
                raise sqlite3.OperationalError("simulated")
            return original(db, query_str, params)

        self.db.do_run_sql_query = failing.__get__(self.db)
        try:
            found, _ = self.search(q)
        finally:
            del self.db.do_run_sql_query
        self.assertEqual(len(calls), 1, "the batch should have been tried")
        self.assertEqual(found, expected)


if __name__ == "__main__":
    unittest.main()
