"""Long chains of ORs must not run into Python's recursion limit (issue #100).

``q = q | Query(...)`` in a loop nests one OR per term. Resolving, searching
and brute-force matching such a query used to recurse once (or more) per
level, so a few hundred to a thousand terms raised RecursionError before the
search ran. Each now walks the chain iteratively.
"""

import os
import random
import sys
import tempfile
import unittest
import uuid

from did.datastructures import field_search, or_operands
from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query

# Comfortably past the default recursion limit of 1000
DEPTH = 3 * sys.getrecursionlimit()


def _old_resolve_structure(ss):
    """The recursive resolver Query used before issue #100, for comparison."""
    if isinstance(ss, list):
        return [_old_resolve_single(item) for item in ss]
    return _old_resolve_single(ss)


def _old_resolve_single(item):
    if not isinstance(item, dict):
        return item
    operation = item.get("operation", "")
    negation = operation.startswith("~")
    op_lower = operation.removeprefix("~").lower()
    if op_lower == "depends_on":
        name, value = item.get("param1", ""), item.get("param2", "")
        keys, values = ["name", "value"], [name, value]
        if name == "*":
            keys, values = ["value"], [value]
        return {
            "field": "depends_on",
            "operation": (
                "~hasanysubfield_exact_string"
                if negation
                else "hasanysubfield_exact_string"
            ),
            "param1": keys,
            "param2": values,
        }
    if op_lower == "or":
        p1, p2 = item.get("param1"), item.get("param2")
        return {
            "field": item.get("field", ""),
            "operation": operation,
            "param1": _old_resolve_structure(p1) if p1 else p1,
            "param2": _old_resolve_structure(p2) if p2 else p2,
        }
    return item


def _random_query(rng, depth):
    """A random query mixing leaves, depends_on, isa, ANDs, ORs and ~ORs."""
    if depth == 0 or rng.random() < 0.3:
        kind = rng.choice(["exact", "isa", "depends_on", "~depends_on", "dep_star"])
        if kind == "exact":
            return Query("base.name", "exact_string", f"n{rng.randrange(5)}")
        if kind == "isa":
            return Query("", "isa", "demoA")
        if kind == "dep_star":
            return Query("", "depends_on", "*", "E1")
        return Query("", kind, "element_id", "E1")
    left, right = _random_query(rng, depth - 1), _random_query(rng, depth - 1)
    combine = rng.choice(["and", "or", "not_or"])
    if combine == "and":
        return left & right
    q = left | right
    if combine == "not_or":
        q.search_structure[0]["operation"] = "~or"
    return q


class TestResolverEquivalence(unittest.TestCase):
    def test_iterative_resolver_matches_recursive_one(self):
        rng = random.Random(100)
        for _ in range(300):
            q = _random_query(rng, depth=6)
            self.assertEqual(
                q.to_search_structure(), _old_resolve_structure(q.search_structure)
            )

    def test_shared_subquery(self):
        # the same object reachable twice must resolve the same both times
        leaf = Query("", "depends_on", "element_id", "E1")
        q = leaf | leaf
        q = q & q
        self.assertEqual(
            q.to_search_structure(), _old_resolve_structure(q.search_structure)
        )


class TestOrOperands(unittest.TestCase):
    def test_flattens_in_order_and_keeps_negated_ors(self):
        a, b, c, d = (Query("base.name", "exact_string", x) for x in "abcd")
        not_cd = c | d
        not_cd.search_structure[0]["operation"] = "~or"
        (struct,) = ((a | b) | not_cd).search_structure
        operands = or_operands(struct)
        self.assertEqual([o[0].get("param1") for o in operands[:2]], ["a", "b"])
        self.assertEqual(operands[2][0]["operation"], "~or")
        self.assertEqual(len(operands), 3)


class TestDeepOr(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.db = SQLiteDB(os.path.join(cls.tmpdir.name, "deep_or.sqlite"))
        cls.db.add_branch("a")
        docs = [
            Document(
                {
                    "base": {
                        "id": uuid.uuid4().hex,
                        "session_id": "S",
                        "name": f"n{i}",
                        "datestamp": "2026-10-24T00:00:00.000Z",
                    },
                    "document_class": {"class_name": "thing", "superclasses": []},
                }
            )
            for i in range(DEPTH)
        ]
        cls.db.add_docs(docs, branch_id="a", validate=False)
        cls.docs = docs
        cls.ids = [d.id() for d in docs]

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        cls.tmpdir.cleanup()

    def chain(self, queries):
        q = queries[0]
        for other in queries[1:]:
            q = q | other  # nested left to right, one level per term
        return q

    def test_or_of_ids_built_in_a_loop(self):
        q = self.chain([Query("base.id", "exact_string", i) for i in self.ids])
        self.assertEqual(sorted(self.db.search(q, branch_id="a")), sorted(self.ids))

    def test_or_of_other_terms_built_in_a_loop(self):
        # not base.id lookups, so _search_doc_ids walks the OR itself
        wanted = self.ids[::3]
        names = [f"n{i}" for i in range(0, DEPTH, 3)]
        q = self.chain([Query("base.name", "exact_string", n) for n in names])
        self.assertEqual(sorted(self.db.search(q, branch_id="a")), sorted(wanted))

    def test_deep_or_with_and(self):
        q = self.chain(
            [Query("base.name", "exact_string", f"n{i}") for i in range(DEPTH)]
        )
        q = q & Query("base.session_id", "exact_string", "S")
        self.assertEqual(len(self.db.search(q, branch_id="a")), DEPTH)

    def test_field_search_on_a_deep_or(self):
        q = self.chain(
            [Query("base.name", "exact_string", f"n{i}") for i in range(DEPTH)]
        )
        (struct,) = q.to_search_structure()
        props = self.docs[-1].document_properties
        self.assertTrue(field_search(props, struct))
        self.assertFalse(field_search({"base": {"name": "nope"}}, struct))


if __name__ == "__main__":
    unittest.main()
