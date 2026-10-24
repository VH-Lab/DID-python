"""depends_on searches run in SQL, with the same results as brute force.

``Query("", "depends_on", name, value)`` resolves to
``hasanysubfield_exact_string`` on ``depends_on``, which used to have no SQL
form: every such search loaded and tested every document of the branch
(issue #101). It is now matched against ``meta.depends_on`` -- doc2sql's
``name1,value1;name2,value2;`` -- with an anchored, case-sensitive ``instr``.
These tests check that the SQL path returns exactly what brute force does on
documents chosen to trip it up, and that it no longer loads documents.
"""

import os
import tempfile
import unittest
import uuid

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query


class _LoadCountingDB(SQLiteDB):
    """A SQLiteDB that counts the documents get_docs is asked for."""

    def __init__(self, filename):
        self.loaded = 0
        super().__init__(filename)

    def get_docs(self, document_ids, *args, **kwargs):
        ids = document_ids if isinstance(document_ids, list) else [document_ids]
        self.loaded += len(ids)
        return super().get_docs(document_ids, *args, **kwargs)


def _doc(depends_on, session="s1"):
    props = {
        "base": {
            "id": uuid.uuid4().hex,
            "session_id": session,
            "name": "",
            "datestamp": "2026-10-24T00:00:00.000Z",
        },
        "document_class": {"class_name": "thing", "superclasses": []},
    }
    if depends_on is not None:
        props["depends_on"] = depends_on
    return Document(props)


def _dep(name, value):
    return {"name": name, "value": value}


class TestDependsOnSearch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.db = _LoadCountingDB(os.path.join(cls.tmpdir.name, "depends_on.sqlite"))
        cls.db.add_branch("a")
        docs = [
            _doc([_dep("element_id", "E1")]),
            _doc([_dep("element_id", "E1"), _dep("epoch_id", "P1")]),
            _doc([_dep("epoch_id", "P1"), _dep("element_id", "E2")]),
            # 'id' must not match inside 'subject_id', nor 'E1' inside 'xE1'
            _doc([_dep("subject_id", "E1")]),
            _doc([_dep("element_id", "xE1")]),
            _doc([_dep("element_id", "E10")]),
            # case matters, as it does for brute force's ==
            _doc([_dep("Element_id", "E1")]),
            _doc([_dep("element_id", "e1")]),
            # enumerated dependency names
            _doc([_dep("input_1", "E1"), _dep("input_2", "E2")]),
            # an empty value is not stored in meta.depends_on
            _doc([_dep("element_id", "")]),
            # no dependencies at all
            _doc([]),
            _doc(None),
        ]
        cls.db.add_docs(docs, branch_id="a", validate=False)
        cls.all_ids = [d.id() for d in docs]

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        cls.tmpdir.cleanup()

    def brute_force(self, query):
        """What brute force returns for a query (one leaf)."""
        (struct,) = query.to_search_structure()
        return sorted(self.db._brute_force_search(struct, "a"))

    def assert_same_as_brute_force(self, name, value):
        for op in ("depends_on", "~depends_on"):
            query = Query("", op, name, value)
            with self.subTest(op=op, name=name, value=value):
                self.assertEqual(
                    sorted(self.db.search(query, branch_id="a")),
                    self.brute_force(query),
                )

    def test_sql_matches_brute_force(self):
        cases = [
            ("element_id", "E1"),
            ("element_id", "E2"),
            ("epoch_id", "P1"),
            ("id", "E1"),  # a suffix of 'element_id' / 'subject_id'
            ("element_id", "1"),  # a suffix of 'E1'
            ("Element_id", "E1"),
            ("element_id", "e1"),
            ("input_2", "E2"),
            ("element_id", "nosuch"),
            ("*", "E1"),
            ("*", "P1"),
            ("*", "1"),
            ("*", "nosuch"),
        ]
        for name, value in cases:
            self.assert_same_as_brute_force(name, value)

    def test_known_matches(self):
        def search(name, value):
            return len(
                self.db.search(Query("", "depends_on", name, value), branch_id="a")
            )

        self.assertEqual(search("element_id", "E1"), 2)
        self.assertEqual(search("id", "E1"), 0)
        self.assertEqual(
            search("*", "E1"), 5
        )  # element_id x2, subject_id, Element_id, input_1

    def test_runs_in_sql_without_loading_documents(self):
        for name, value in (("element_id", "E1"), ("*", "P1")):
            self.db.loaded = 0
            self.db.search(Query("", "depends_on", name, value), branch_id="a")
            self.assertEqual(self.db.loaded, 0, f"depends_on {name} {value}")

    def test_what_sql_cannot_represent_still_brute_forces(self):
        # an empty value is not stored in meta.depends_on, and ',' / ';' are
        # its separators: these keep the brute-force path, and its results
        for name, value in (("element_id", ""), ("element_id", "a,b"), ("a;b", "E1")):
            query = Query("", "depends_on", name, value)
            self.db.loaded = 0
            found = sorted(self.db.search(query, branch_id="a"))
            self.assertGreater(
                self.db.loaded, 0, f"{name!r} {value!r} should brute-force"
            )
            self.assertEqual(found, self.brute_force(query))
        self.assertEqual(
            len(
                self.db.search(Query("", "depends_on", "element_id", ""), branch_id="a")
            ),
            1,
        )

    def test_depends_on_with_session(self):
        q = Query("", "depends_on", "element_id", "E1") & Query(
            "base.session_id", "exact_string", "s1"
        )
        self.db.loaded = 0
        self.assertEqual(len(self.db.search(q, branch_id="a")), 2)
        self.assertEqual(self.db.loaded, 0)


if __name__ == "__main__":
    unittest.main()
