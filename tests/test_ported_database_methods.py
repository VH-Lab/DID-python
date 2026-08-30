"""The Not Yet Ported backlog: methods MATLAB had and Python did not.

Covers DID-python#53 — display_branches, close_doc, the three preference
accessors, and the one item on that list that *differed* rather than being
absent: which operator a search cell array picks for a non-scalar value.
"""

import io
import os
import shutil
import unittest
from contextlib import redirect_stdout

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query


class TestPreferenceAccessors(unittest.TestCase):
    """MATLAB has get/set_preference and get_preference_names; the dict was
    here with no way to reach it except by touching the attribute."""

    def setUp(self):
        self.db = SQLiteDB(":memory:")

    def test_set_and_get_a_preference(self):
        self.db.set_preference("cache_folder", "/tmp/x")
        self.assertEqual(self.db.get_preference("cache_folder"), "/tmp/x")

    def test_names_are_empty_until_something_is_set(self):
        self.assertEqual(self.db.get_preference_names(), [])
        self.db.set_preference("a", 1)
        self.db.set_preference("b", 2)
        self.assertCountEqual(self.db.get_preference_names(), ["a", "b"])

    def test_an_unset_preference_raises_without_a_default(self):
        with self.assertRaises(ValueError) as caught:
            self.db.get_preference("nope")
        self.assertIn("not defined", str(caught.exception))

    def test_an_unset_preference_returns_the_default_when_given(self):
        self.assertEqual(self.db.get_preference("nope", "fallback"), "fallback")

    def test_a_stored_none_is_not_the_same_as_unset(self):
        """Why the default is *args rather than default=None: MATLAB stores []
        for a valueless set_preference, and a stored empty must be
        distinguishable from never having been set."""
        self.db.set_preference("explicitly_none")
        self.assertIsNone(self.db.get_preference("explicitly_none"))
        self.assertIn("explicitly_none", self.db.get_preference_names())

        # ...whereas an unset name still raises
        with self.assertRaises(ValueError):
            self.db.get_preference("never_set")

    def test_a_bad_preference_name_raises(self):
        for bad in ("", None, 42):
            with self.assertRaises(ValueError):
                self.db.set_preference(bad, 1)
            with self.assertRaises(ValueError):
                self.db.get_preference(bad)


class TestDisplayBranches(unittest.TestCase):
    DB_FILENAME = "test_display_branches.sqlite"

    def setUp(self):
        self.db_path = os.path.join(os.path.dirname(__file__), self.DB_FILENAME)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = SQLiteDB(self.db_path)
        self.db.add_branch("a")
        self.db.add_branch("a_a", "a")
        self.db.add_branch("a_a_a", "a_a")
        self.db.add_branch("a_b", "a")

    def tearDown(self):
        self.db._close_db()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _display(self, branch_id=None):
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.db.display_branches(branch_id)
        return buf.getvalue().splitlines()

    def test_the_hierarchy_is_printed_with_indentation(self):
        lines = self._display("a")
        self.assertEqual(lines[0], " - a")
        # children are indented one level, the grandchild two
        self.assertIn("   - a_a", lines)
        self.assertIn("   - a_b", lines)
        self.assertIn("     - a_a_a", lines)

    def test_a_leaf_prints_only_itself(self):
        self.assertEqual(self._display("a_b"), [" - a_b"])

    def test_it_defaults_to_the_current_branch(self):
        self.db.set_branch("a_a")
        self.assertEqual(self._display(), [" - a_a", "   - a_a_a"])

    def test_a_missing_branch_raises(self):
        with self.assertRaises(ValueError):
            self.db.display_branches("no_such_branch")


class TestCloseDoc(unittest.TestCase):
    """MATLAB's close_doc; Python had none. Its file objects also close on
    garbage collection, so this is the explicit call, not a leak fix."""

    DB_FILENAME = "test_close_doc.sqlite"

    def setUp(self):
        self.db_path = os.path.join(os.path.dirname(__file__), self.DB_FILENAME)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = SQLiteDB(self.db_path)
        self.db.add_branch("a")
        Document.set_schema_path(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "src",
                "did",
                "example_schema",
                "demo_schema1",
            )
        )

    def tearDown(self):
        self.db._close_db()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        shutil.rmtree(
            os.path.join(os.path.dirname(self.db_path), "files"), ignore_errors=True
        )

    def test_close_doc_closes_an_open_file_object(self):
        doc = Document("demoFile")
        source = os.path.join(os.path.dirname(self.db_path), "close_me.txt")
        with open(source, "w") as handle:
            handle.write("data")
        doc.add_file("test_file.txt", source)
        self.db.add_docs([doc], validate=False)

        file_obj = self.db.open_doc(doc.id(), "test_file.txt")
        file_obj.fopen()
        self.assertIsNotNone(file_obj.fid, "setup: the file should be open")

        self.db.close_doc(file_obj)
        self.assertIsNone(file_obj.fid)

    def test_close_doc_tolerates_none_and_an_already_closed_object(self):
        self.db.close_doc(None)  # must not raise

        doc = Document("demoFile")
        source = os.path.join(os.path.dirname(self.db_path), "close_me2.txt")
        with open(source, "w") as handle:
            handle.write("data")
        doc.add_file("test_file.txt", source)
        self.db.add_docs([doc], validate=False)

        file_obj = self.db.open_doc(doc.id(), "test_file.txt")
        file_obj.fopen()
        self.db.close_doc(file_obj)
        self.db.close_doc(file_obj)  # idempotent
        self.assertIsNone(file_obj.fid)


class TestSearchCellArrayOperator(unittest.TestCase):
    """MATLAB branches on ischar; this branched on int/float, so a list or
    dict silently got regexp where MATLAB gives exact_number."""

    def _op(self, value):
        return Query.search_cell_array_to_search_structure(["f", value])[0]["operation"]

    def test_a_string_gets_regexp(self):
        self.assertEqual(self._op("abc"), "regexp")

    def test_numbers_get_exact_number(self):
        self.assertEqual(self._op(5), "exact_number")
        self.assertEqual(self._op(1.5), "exact_number")

    def test_a_logical_gets_exact_number(self):
        """Agreed before only because bool subclasses int. Pinned so it stays
        true if the branch is ever rewritten."""
        self.assertEqual(self._op(True), "exact_number")

    def test_a_list_or_dict_gets_exact_number(self):
        """The actual divergence: these used to get regexp."""
        self.assertEqual(self._op([1, 2]), "exact_number")
        self.assertEqual(self._op({"a": 1}), "exact_number")


if __name__ == "__main__":
    unittest.main()
