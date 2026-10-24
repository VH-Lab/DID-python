"""The SQLite search applies the negation of a '~or' (DID-matlab#237).

DID-matlab's did.database/search_doc_ids used to strip the '~' of a '~or' and
search it as a plain 'or'. SQLiteDB._search_doc_ids has always taken the
complement (``universe - matched``); these tests pin that down with the same
cases as DID-matlab's did.unittest.TestNegatedOr: every search is compared
with ``field_search`` applied to each document of the branch, with and without
the search indexes (without them there are no candidate documents). The
structures use no 'isa', which field_search knows only as
``Query.to_search_structure`` expands it.
"""

import os
import tempfile
import unittest
import uuid

from did.datastructures import field_search
from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query

NUM_A = 30  # demoA documents
NUM_B = 20  # demoB documents (also demoA)


def _doc(class_name, superclasses, name, values):
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
    }
    props.update(values)
    return Document(props)


def _not_or(a, b):
    """~or(a, b): did.query, like Query, has no way to build one."""
    q = a | b
    q.search_structure[0]["operation"] = "~or"
    return q


def _name_is(name):
    return Query("base.name", "exact_string", name)


def _is_a():
    # every document (a field term stands in for a class test)
    return Query("demoA.value", "greaterthan", 0)


def _is_b():
    # the demoB documents
    return Query("demoB.value", "greaterthan", 0)


class _CountingDB(SQLiteDB):
    """A SQLiteDB that counts the times it lists every document of a branch."""

    def __init__(self, filename):
        self.branch_listings = 0
        super().__init__(filename)

    def _do_get_doc_ids(self, *args, **kwargs):
        self.branch_listings += 1
        return super()._do_get_doc_ids(*args, **kwargs)


class TestNegatedOr(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.db = _CountingDB(os.path.join(cls.tmpdir.name, "negated_or.sqlite"))
        cls.db.add_branch("a")
        docs = [
            _doc("demoA", ["base"], f"n{i}", {"demoA": {"value": i}})
            for i in range(1, NUM_A + 1)
        ] + [
            _doc(
                "demoB",
                ["base", "demoA"],
                f"b{i}",
                {"demoA": {"value": i}, "demoB": {"value": 100 + i}},
            )
            for i in range(1, NUM_B + 1)
        ]
        cls.db.add_docs(docs, branch_id="a", validate=False)
        cls.ids = [d.id() for d in docs]
        cls.props = [d.document_properties for d in docs]

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        cls.tmpdir.cleanup()

    def tearDown(self):
        self.db._search_indexes_ready = True

    def id_is(self, k):
        """base.id of the k-th document (1-based, as in the MATLAB test)."""
        return Query("base.id", "exact_string", self.ids[k - 1])

    def field_search_ids(self, q):
        s = q.to_search_structure()
        return sorted(i for i, p in zip(self.ids, self.props) if field_search(p, s))

    def assert_matches_field_search(self, q, msg):
        expected = self.field_search_ids(q)
        self.assertEqual(
            sorted(self.db.search(q, "a")), expected, f"{msg}: with the search indexes"
        )
        self.db._search_indexes_ready = False
        try:
            found = sorted(self.db.search(q, "a"))
        finally:
            self.db._search_indexes_ready = True
        self.assertEqual(found, expected, f"{msg}: without the search indexes")

    def test_top_level_negated_or(self):
        q = _not_or(_name_is("n1"), _name_is("b2"))
        self.assert_matches_field_search(q, "~or(n1, b2)")
        found = self.db.search(q, "a")
        self.assertEqual(len(found), len(self.ids) - 2)
        self.assertFalse({self.ids[0], self.ids[NUM_A + 1]} & set(found))

        q = _not_or(_is_b(), Query("demoA.value", "lessthan", 5))
        self.assert_matches_field_search(q, "~or(demoB, value < 5)")
        q = _not_or(_name_is("nope"), _name_is("nope2"))
        self.assert_matches_field_search(q, "~or of names that do not exist")
        q = _not_or(_is_a(), _name_is("n1"))
        self.assert_matches_field_search(q, "~or that matches every document")

    def test_negated_or_nested_in_or(self):
        q = _name_is("n3") | _not_or(_is_b(), Query("demoA.value", "greaterthan", 10))
        self.assert_matches_field_search(q, "n3 OR ~or(demoB, value > 10)")

        inner = _not_or(_is_a(), _name_is("n1"))
        q = (_name_is("n4") | _name_is("b5")) | _not_or(
            _name_is("n6") | _name_is("n7"), inner
        )
        self.assert_matches_field_search(q, "a ~or of a ~or, nested in an OR")

    def test_negated_or_inside_and(self):
        # the demoB term leaves 20 candidates, so the ~or is searched
        # restricted to them: the candidates minus the OR's matches
        not_or_term = _not_or(_name_is("b1"), Query("demoB.value", "greaterthan", 115))
        q = _is_b() & not_or_term
        self.assert_matches_field_search(q, "demoB AND ~or(b1, demoB.value > 115)")

        self.db.branch_listings = 0
        self.db.search(q, "a")
        self.assertEqual(
            self.db.branch_listings,
            0,
            "restricted to candidates, the ~or must not list every document",
        )

        q = not_or_term & _is_b()
        self.assert_matches_field_search(q, "~or(b1, demoB.value > 115) AND demoB")
        q = _is_b() & (_name_is("b2") | not_or_term)
        self.assert_matches_field_search(q, "demoB AND (b2 OR ~or(...))")

    def test_negated_or_of_base_ids(self):
        i_b = NUM_A + 3
        q = _not_or(self.id_is(2), self.id_is(i_b))
        self.assert_matches_field_search(q, "~or of base.id lookups")
        found = self.db.search(q, "a")
        self.assertFalse({self.ids[1], self.ids[i_b - 1]} & set(found))

        q = _not_or(
            self.id_is(5) | Query("base.id", "exact_string", "no_such_id"),
            self.id_is(6),
        )
        self.assert_matches_field_search(q, "~or of a nested OR of base.id lookups")
        q = self.id_is(1) | q
        self.assert_matches_field_search(q, "base.id OR ~or of base.id lookups")

        q = _is_b() & _not_or(self.id_is(i_b), self.id_is(1))
        self.assert_matches_field_search(q, "demoB AND ~or of base.id lookups")
        q = self.id_is(i_b) & _not_or(self.id_is(i_b), self.id_is(1))
        self.assert_matches_field_search(q, "base.id AND ~or of that base.id")


if __name__ == "__main__":
    unittest.main()
