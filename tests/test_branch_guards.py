"""delete_branch refuses what MATLAB's delete_branch refuses.

Until this was ported, Python's delete_branch was a bare delegate behind a
"validation logic would go here" placeholder, so all three of MATLAB's guards
were missing and the current branch was left dangling afterwards. Each test
below names the behavior before the port.

Covers database.delete_branch, freeze_branch and is_branch_editable.
"""

import os
import sqlite3
import unittest

from did.implementations.sqlitedb import SQLiteDB


class TestBranchGuards(unittest.TestCase):
    DB_FILENAME = "test_branch_guards.sqlite"

    def setUp(self):
        self.db_path = os.path.join(os.path.dirname(__file__), self.DB_FILENAME)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = SQLiteDB(self.db_path)
        self.db.add_branch("a")

    def tearDown(self):
        self.db._close_db()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    # -- guard 1: the branch must exist -----------------------------------

    def test_deleting_a_missing_branch_raises(self):
        """Previously a silent no-op: nothing raised and nothing changed, so a
        script deleting the wrong id got no signal at all."""
        with self.assertRaises(ValueError) as caught:
            self.db.delete_branch("no_such_branch")
        self.assertIn("does not exist", str(caught.exception))

        # the real branch is untouched
        self.assertIn("a", self.db.all_branch_ids())

    def test_a_branch_id_that_is_not_a_string_raises(self):
        with self.assertRaises(ValueError) as caught:
            self.db.delete_branch(42)
        self.assertIn("non-empty string", str(caught.exception))

    def test_an_empty_id_with_no_current_branch_raises(self):
        """ "" and None mean "the current branch", which is only usable when
        there is one. With none set, the id really is empty."""
        self.db.set_branch("")
        with self.assertRaises(ValueError) as caught:
            self.db.delete_branch()
        self.assertIn("non-empty string", str(caught.exception))

    # -- guard 2: the branch must have no sub-branches ---------------------

    def test_deleting_a_parent_branch_raises_a_described_error(self):
        """Previously this surfaced only as the FOREIGN KEY failing, i.e. a
        raw sqlite3.IntegrityError with no indication of the cause."""
        self.db.add_branch("a_a", "a")

        with self.assertRaises(ValueError) as caught:
            self.db.delete_branch("a")
        self.assertIn("sub-branches", str(caught.exception))
        self.assertNotIsInstance(caught.exception, sqlite3.IntegrityError)

        # both branches survive the refusal
        self.assertCountEqual(self.db.all_branch_ids(), ["a", "a_a"])

        # and once the child is gone, the parent can be deleted
        self.db.delete_branch("a_a")
        self.db.delete_branch("a")
        self.assertEqual(self.db.all_branch_ids(), [])

    # -- guard 3: the branch must not be frozen ---------------------------

    def test_deleting_a_frozen_branch_raises(self):
        """freeze_branch had no Python counterpart, so nothing was refused."""
        self.db.freeze_branch("a")

        with self.assertRaises(ValueError) as caught:
            self.db.delete_branch("a")
        self.assertIn("frozen", str(caught.exception))
        self.assertIn("a", self.db.all_branch_ids())

    def test_freeze_branch_defaults_to_the_current_branch(self):
        self.db.add_branch("a_a", "a")  # current becomes 'a_a'
        self.db.freeze_branch()
        self.assertEqual(self.db.frozen_branch_ids, ["a_a"])

    def test_freezing_is_idempotent_and_validated(self):
        self.db.freeze_branch("a")
        self.db.freeze_branch("a")
        self.assertEqual(self.db.frozen_branch_ids, ["a"])

        with self.assertRaises(ValueError):
            self.db.freeze_branch("no_such_branch")

    # -- is_branch_editable -----------------------------------------------

    def test_is_branch_editable_reports_both_conditions(self):
        self.assertTrue(self.db.is_branch_editable("a"))

        # a parent is not editable
        self.db.add_branch("a_a", "a")
        self.assertFalse(self.db.is_branch_editable("a"))
        self.assertTrue(self.db.is_branch_editable("a_a"))

        # nor is a frozen leaf
        self.db.freeze_branch("a_a")
        self.assertFalse(self.db.is_branch_editable("a_a"))

    # -- the current branch is not left dangling ---------------------------

    def test_deleting_the_current_branch_moves_the_current_branch(self):
        """Previously current_branch_id kept naming the deleted branch, so the
        next add_branch inherited a parent that no longer existed and failed
        on the FOREIGN KEY."""
        self.db.add_branch("b", "")  # a second root; current becomes 'b'
        self.assertEqual(self.db.current_branch_id, "b")

        self.db.delete_branch("b")

        self.assertNotEqual(self.db.current_branch_id, "b")
        self.assertIn(self.db.current_branch_id, self.db.all_branch_ids())

        # the fallback branch is usable as a parent straight away
        self.db.add_branch("c")
        self.assertCountEqual(self.db.all_branch_ids(), ["a", "c"])

    def test_deleting_the_last_branch_leaves_no_current_branch(self):
        self.db.delete_branch("a")
        self.assertEqual(self.db.current_branch_id, "")
        self.assertEqual(self.db.all_branch_ids(), [])

    def test_deleting_a_branch_that_is_not_current_leaves_current_alone(self):
        self.db.add_branch("b", "")  # current becomes 'b'
        self.db.delete_branch("a")
        self.assertEqual(self.db.current_branch_id, "b")

    def test_delete_branch_defaults_to_the_current_branch(self):
        self.db.add_branch("b", "")
        self.assertEqual(self.db.current_branch_id, "b")

        self.db.delete_branch()

        self.assertEqual(self.db.all_branch_ids(), ["a"])
        self.assertEqual(self.db.current_branch_id, "a")


if __name__ == "__main__":
    unittest.main()
