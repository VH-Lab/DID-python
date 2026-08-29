"""Atomicity and OnDuplicate tests for ``SQLiteDB._do_add_doc``.

Two closely-coupled correctness fixes that both restructure ``_do_add_doc``:

* A failed add to a nonexistent branch used to insert the docs/doc_data rows
  BEFORE the branch_docs FOREIGN KEY check failed, and never rolled back — so
  the orphan rows sat in the open transaction and were committed by the next
  successful add (a document belonging to no branch was reported present).
* Re-adding a document already on a branch silently swallowed the duplicate
  (``except sqlite3.IntegrityError: pass``), keeping stale content, where
  DID-matlab errors by default. ``OnDuplicate`` in {error(default),warn,ignore}
  now governs that path.
"""

import os
import unittest
import warnings

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB


def _make_doc(idhex, value=1):
    return Document(
        {
            "base": {"id": idhex, "datestamp": "2020-01-01"},
            "document_class": {"class_name": "thing"},
            "demoA": {"value": value},
        }
    )


# Every add_docs call below passes validate=False: these are hand-built stub
# documents that exercise the add path's transaction and duplicate semantics,
# not the schema validator (which landed on main after this work was written
# and would reject them for a missing document_class.property_list_name).


class TestAddDocsAtomicity(unittest.TestCase):
    DB_FILENAME = "test_add_docs_atomicity.sqlite"

    def setUp(self):
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)
        self.db = SQLiteDB(self.DB_FILENAME)
        self.db.add_branch("a")

    def tearDown(self):
        self.db._close_db()
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)

    def test_failed_add_to_missing_branch_leaves_no_orphan(self):
        z = _make_doc("a" * 32)

        # Add to a nonexistent branch: must raise ...
        with self.assertRaises(ValueError):
            self.db.add_docs([z], branch_id="nonexistent", validate=False)

        # ... and a SUBSEQUENT SUCCESSFUL add must commit. Without this second
        # add the orphan row would merely be uncommitted and the assertion below
        # would pass spuriously; the second commit is what would have flushed a
        # leaked orphan to disk under the old code.
        other = _make_doc("b" * 32)
        self.db.add_docs([other], branch_id="a", validate=False)

        all_ids = self.db.all_doc_ids()
        self.assertNotIn(z.id(), all_ids)
        self.assertIn(other.id(), all_ids)

    def test_duplicate_in_branch_raises_by_default(self):
        d = _make_doc("c" * 32)
        self.db.add_docs([d], branch_id="a", validate=False)
        with self.assertRaises(ValueError):
            self.db.add_docs([d], branch_id="a", validate=False)

    def test_duplicate_onduplicate_ignore_does_not_raise(self):
        d = _make_doc("d" * 32)
        self.db.add_docs([d], branch_id="a", validate=False)
        # Must not raise and must leave exactly one entry.
        self.db.add_docs([d], branch_id="a", validate=False, OnDuplicate="ignore")
        self.assertEqual(self.db.all_doc_ids().count(d.id()), 1)

    def test_duplicate_onduplicate_warn_warns(self):
        d = _make_doc("e" * 32)
        self.db.add_docs([d], branch_id="a", validate=False)
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # ensure a warning is actually raised
            with self.assertRaises(UserWarning):
                self.db.add_docs([d], branch_id="a", validate=False, OnDuplicate="warn")

    def test_rejects_invalid_onduplicate_value(self):
        d = _make_doc("f" * 32)
        with self.assertRaises(ValueError):
            self.db.add_docs([d], branch_id="a", validate=False, OnDuplicate="bogus")

    def test_adding_existing_doc_to_new_branch_is_not_a_duplicate(self):
        # A doc already in docs (from branch 'a') added to a DIFFERENT branch is
        # a legitimate cross-branch add, not a duplicate, and must not raise.
        d = _make_doc("1" * 32)
        self.db.add_docs([d], branch_id="a", validate=False)
        self.db.add_branch("b", parent_branch_id="")
        self.db.add_docs([d], branch_id="b", validate=False)
        self.assertIn(d.id(), self.db.get_doc_ids("b"))


if __name__ == "__main__":
    unittest.main()
