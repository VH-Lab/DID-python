"""add_branch and delete_branch refuse what MATLAB refuses.

Both were bare delegates behind "validation logic would go here"
placeholders, so MATLAB's guards were missing on each. Each test below names
the behavior before the port.

Covers database.add_branch, delete_branch, freeze_branch and
is_branch_editable.
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

    # -- add_branch ---------------------------------------------------------

    def test_adding_an_empty_branch_id_raises(self):
        """Previously accepted, and the damage was downstream: "" is also the
        sentinel for *no* current branch, so the branch was created, made
        current, and the next add_branch read that parent, turned it into
        NULL and silently produced a ROOT instead of a child."""
        with self.assertRaises(ValueError) as caught:
            self.db.add_branch("")
        self.assertIn("non-empty string", str(caught.exception))

        self.assertEqual(self.db.all_branch_ids(), ["a"])
        self.assertEqual(self.db.current_branch_id, "a")

    def test_adding_a_non_string_branch_id_raises(self):
        """Previously accepted: SQLite's TEXT affinity stored '42' while
        current_branch_id became the integer 42, and an integer never compares
        equal to a text value — so the current branch named nothing and
        get_doc_ids on it returned [] rather than raising."""
        with self.assertRaises(ValueError) as caught:
            self.db.add_branch(42)
        self.assertIn("non-empty string", str(caught.exception))

        self.assertEqual(self.db.all_branch_ids(), ["a"])
        self.assertEqual(self.db.current_branch_id, "a")

    def test_adding_a_duplicate_branch_id_raises_a_described_error(self):
        """Previously refused only by the UNIQUE constraint, i.e. a raw
        sqlite3.IntegrityError with no indication of the cause."""
        with self.assertRaises(ValueError) as caught:
            self.db.add_branch("a")
        self.assertIn("already exists", str(caught.exception))
        self.assertNotIsInstance(caught.exception, sqlite3.IntegrityError)

    def test_adding_under_a_missing_parent_raises_a_described_error(self):
        """Previously refused only by the FOREIGN KEY, same problem."""
        with self.assertRaises(ValueError) as caught:
            self.db.add_branch("x", "no_such_parent")
        self.assertIn("does not exist", str(caught.exception))
        self.assertNotIsInstance(caught.exception, sqlite3.IntegrityError)

        self.assertEqual(self.db.all_branch_ids(), ["a"])
        self.assertEqual(self.db.current_branch_id, "a")

    def test_an_omitted_or_empty_parent_means_the_current_branch(self):
        """MATLAB's isempty() covers both [] and '', so the two are the same
        request. They were not here: an explicit "" used to mean "no parent"
        and produced a root."""
        self.db.add_branch("b")
        self.assertEqual(self.db.get_branch_parent("b"), "a")
        self.assertEqual(self.db.current_branch_id, "b")

        self.db.set_branch("a")
        self.db.add_branch("c", "")
        self.assertEqual(self.db.get_branch_parent("c"), "a")

    def test_a_root_branch_needs_there_to_be_no_current_branch(self):
        """Which is how the first branch of a fresh database is made, and --
        now that set_branch cannot clear the current branch -- the only way
        either language can make one at all."""
        self.assertIsNone(self.db.get_branch_parent("a"))

        # the only remaining route to "no current branch" is deleting the last
        self.db.delete_branch("a")
        self.assertEqual(self.db.current_branch_id, "")

        self.db.add_branch("r")
        self.assertIsNone(self.db.get_branch_parent("r"))
        self.assertEqual(self.db.current_branch_id, "r")

    def test_a_second_root_cannot_be_added(self):
        """Not a Python limitation: MATLAB cannot do it either, for the same
        reason. Recorded so the constraint is visible rather than folklore.
        See VH-Lab/DID-matlab#165."""
        with self.assertRaises(ValueError):
            self.db.set_branch("")  # the only way to ask for "no parent"

        self.db.add_branch("b")
        self.assertEqual(self.db.get_branch_parent("b"), "a")

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
        self.db.delete_branch("a")  # leaves no current branch
        with self.assertRaises(ValueError) as caught:
            self.db.delete_branch()
        self.assertIn("non-empty string", str(caught.exception))

    # -- set_branch -------------------------------------------------------

    def test_set_branch_refuses_a_branch_that_does_not_exist(self):
        """Previously accepted: current_branch_id was assigned whatever it was
        given, and the mistake surfaced later at the next add_docs. MATLAB
        fails fast here."""
        with self.assertRaises(ValueError) as caught:
            self.db.set_branch("no_such_branch")
        self.assertIn("does not exist", str(caught.exception))
        self.assertEqual(self.db.current_branch_id, "a")

    def test_set_branch_refuses_an_empty_or_non_string_id(self):
        for bad in ("", 42):
            with self.assertRaises(ValueError) as caught:
                self.db.set_branch(bad)
            self.assertIn("non-empty string", str(caught.exception))
        self.assertEqual(self.db.current_branch_id, "a")

    def test_set_branch_still_moves_to_a_real_branch(self):
        self.db.add_branch("b")  # current becomes 'b'
        self.db.set_branch("a")
        self.assertEqual(self.db.current_branch_id, "a")

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
        self.db.add_branch("b")  # child of 'a'; current becomes 'b'
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
        # two leaves under 'a', so the one deleted is neither current nor a
        # parent
        self.db.add_branch("b")
        self.db.set_branch("a")
        self.db.add_branch("c")  # current becomes 'c'

        self.db.delete_branch("b")

        self.assertEqual(self.db.current_branch_id, "c")
        self.assertCountEqual(self.db.all_branch_ids(), ["a", "c"])

    def test_delete_branch_defaults_to_the_current_branch(self):
        self.db.add_branch("b")
        self.assertEqual(self.db.current_branch_id, "b")

        self.db.delete_branch()

        self.assertEqual(self.db.all_branch_ids(), ["a"])
        self.assertEqual(self.db.current_branch_id, "a")


if __name__ == "__main__":
    unittest.main()
