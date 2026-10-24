"""depends_on means the same thing on every search path, and in MATLAB.

``Query("", "depends_on", name, value)`` is "the document has a dependency
named ``name`` with value ``value``", where only the name ``'*'`` is a
wildcard (any name); matching is exact and case-sensitive. Each case below is
run on the SQLite search and on brute force (field_search), and both must
return the documents listed.

The same documents and cases are in DID-matlab's
tests/+did/+unittest/TestDependsOnParity.m. Keep the two identical, so that
the languages cannot drift apart again: DID-matlab's SQL search used to match
``LIKE "%name,value;%"``, under which an empty name matched any name, ``id``
also matched ``subject_id`` and ``ID`` matched ``id``.
"""

import os
import tempfile
import unittest
import uuid

from did.datastructures import field_search
from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query

# label -> [(dependency name, dependency value), ...]
DOCUMENTS = {
    "A": [("subject_id", "X")],
    "B": [("id", "X"), ("element_id", "Y")],
    "C": [("ID", "X")],
    "D": [],
    "E": [("element_id", "x")],
}

# label -> (operation, name, value, expected documents)
CASES = {
    "emptyName": ("depends_on", "", "X", []),
    "starName": ("depends_on", "*", "X", ["A", "B", "C"]),
    "exactName": ("depends_on", "id", "X", ["B"]),
    "longerName": ("depends_on", "subject_id", "X", ["A"]),
    "nameCase": ("depends_on", "ID", "X", ["C"]),
    "valueCase": ("depends_on", "element_id", "X", []),
    "valueCaseLower": ("depends_on", "element_id", "x", ["E"]),
    "secondDependency": ("depends_on", "element_id", "Y", ["B"]),
    "starSecondDependency": ("depends_on", "*", "Y", ["B"]),
    "nameValueApart": ("depends_on", "id", "Y", []),
    "starIsNotGlob": ("depends_on", "subject*", "X", []),
    "noSuchValue": ("depends_on", "*", "Z", []),
    "notExactName": ("~depends_on", "id", "X", ["A", "C", "D", "E"]),
    "notStarName": ("~depends_on", "*", "X", ["D", "E"]),
    "notEmptyName": ("~depends_on", "", "X", ["A", "B", "C", "D", "E"]),
    "notNameCase": ("~depends_on", "ID", "X", ["A", "B", "D", "E"]),
}


def _doc(dependencies):
    return Document(
        {
            "base": {
                "id": uuid.uuid4().hex,
                "session_id": "s1",
                "name": "",
                "datestamp": "2026-10-24T00:00:00.000Z",
            },
            "document_class": {"class_name": "demoC", "superclasses": []},
            "depends_on": [{"name": n, "value": v} for n, v in dependencies],
        }
    )


class TestDependsOnParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.db = SQLiteDB(os.path.join(cls.tmpdir.name, "depends_on_parity.sqlite"))
        cls.db.add_branch("a")
        cls.docs = {label: _doc(deps) for label, deps in DOCUMENTS.items()}
        cls.db.add_docs(list(cls.docs.values()), branch_id="a", validate=False)

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        cls.tmpdir.cleanup()

    def expected_ids(self, labels):
        return sorted(self.docs[label].id() for label in labels)

    def test_sqlite_search(self):
        for label, (op, name, value, expected) in CASES.items():
            with self.subTest(label):
                found = self.db.search(Query("", op, name, value), branch_id="a")
                self.assertEqual(sorted(found), self.expected_ids(expected))

    def test_field_search(self):
        for label, (op, name, value, expected) in CASES.items():
            with self.subTest(label):
                structs = Query("", op, name, value).to_search_structure()
                found = [
                    doc.id()
                    for doc in self.docs.values()
                    if field_search(doc.document_properties, structs)
                ]
                self.assertEqual(sorted(found), self.expected_ids(expected))

    def test_sqlite_search_within_and(self):
        # The same, as the second term of an AND: then it is checked against
        # the documents the first term found only
        for label, (op, name, value, expected) in CASES.items():
            with self.subTest(label):
                q = Query("", "isa", "demoC") & Query("", op, name, value)
                found = self.db.search(q, branch_id="a")
                self.assertEqual(sorted(found), self.expected_ids(expected))

    def test_two_dependencies(self):
        q = Query("", "depends_on", "id", "X") & Query(
            "", "depends_on", "element_id", "Y"
        )
        self.assertEqual(self.db.search(q, branch_id="a"), [self.docs["B"].id()])


if __name__ == "__main__":
    unittest.main()
