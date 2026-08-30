"""An explicit branch_id of "" means the current branch, as MATLAB's isempty does.

Six Database methods guarded their branch argument with ``is None``, which does
not cover ``""``. MATLAB guards with ``isempty()``, which covers both ``[]`` and
``''``, and then validates. The mismatch let an empty string reach the ``_do_*``
layer, where each method made its own decision about a falsy id -- and they did
not agree with each other or with MATLAB. See issue #55.

Every fixture below puts documents on MORE THAN ONE branch. With a single
branch the all-branches query and the one-branch query return the same rows, so
the interesting cases would pass for the wrong reason.
"""

import os
import tempfile
import unittest

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query


def _make_doc(idhex):
    return Document(
        {
            "base": {"id": idhex, "datestamp": "2020-01-01"},
            "document_class": {"class_name": "thing"},
            "demoA": {"value": 1},
        }
    )


class TestBranchIdDefaults(unittest.TestCase):
    def setUp(self):
        self.db = SQLiteDB(os.path.join(tempfile.mkdtemp(), "t.sqlite"))
        # 'a' holds one document; 'b' is a child of 'a' and holds a second.
        # Because add_branch copies branch_docs rows at creation time, 'b'
        # inherits a's document as well -- so the branches genuinely differ:
        # 'a' has 1 document, 'b' has 2, the database has 2.
        self.db.add_branch("a")
        self.db.add_docs([_make_doc("a" * 32)], branch_id="a", validate=False)
        self.db.add_branch("b", "a")
        self.db.add_docs([_make_doc("b" * 32)], branch_id="b", validate=False)
        self.db.set_branch("a")

    def tearDown(self):
        self.db._close_db()

    def _query(self):
        return Query("base.id", "hasfield", "", "")

    # -- the fixture is what makes the rest meaningful ---------------------

    def test_the_fixture_really_does_span_two_branches(self):
        """If this ever stops holding, every "" test below goes vacuous."""
        self.assertEqual(len(self.db.get_doc_ids("a")), 1)
        self.assertEqual(len(self.db.get_doc_ids("b")), 2)
        self.assertEqual(len(self.db.all_doc_ids()), 2)

    # -- "" means the current branch ---------------------------------------

    def test_get_doc_ids_empty_string_means_the_current_branch(self):
        """The worst of the six: "" used to return every document in the
        database. _do_get_doc_ids guards on truthiness, so an empty id dropped
        the branch filter and ran the all-branches query -- get_doc_ids("") was
        all_doc_ids()."""
        self.assertEqual(self.db.get_doc_ids(""), self.db.get_doc_ids("a"))
        self.assertEqual(len(self.db.get_doc_ids("")), 1)

    def test_search_empty_string_means_the_current_branch(self):
        """SQLiteDB overrides Database.search, so this needed fixing in both
        places; fixing only the base class would have left this path alone."""
        self.assertEqual(
            self.db.search(self._query(), ""), self.db.search(self._query(), "a")
        )
        self.assertEqual(len(self.db.search(self._query(), "")), 1)

    def test_get_sub_branches_empty_string_means_the_current_branch(self):
        """This one failed in the opposite direction to get_doc_ids: the SELECT
        matched no parent_id and returned [] rather than too much."""
        self.assertEqual(self.db.get_sub_branches(""), ["b"])

    def test_get_branch_parent_empty_string_means_the_current_branch(self):
        self.db.set_branch("b")
        self.assertEqual(self.db.get_branch_parent(""), "a")

    def test_remove_docs_empty_string_means_the_current_branch(self):
        self.db.set_branch("b")
        self.db.remove_docs("b" * 32, "")
        self.assertEqual(len(self.db.get_doc_ids("b")), 1)
        # and 'a' is untouched: removing from one branch is not removing from all
        self.assertEqual(len(self.db.get_doc_ids("a")), 1)

    def test_add_docs_empty_string_means_the_current_branch(self):
        self.db.set_branch("b")
        self.db.add_docs([_make_doc("c" * 32)], branch_id="", validate=False)
        self.assertIn("c" * 32, self.db.get_doc_ids("b"))
        self.assertNotIn("c" * 32, self.db.get_doc_ids("a"))

    # -- and a branch that does not exist is refused ------------------------

    def test_reads_refuse_a_branch_that_does_not_exist(self):
        """Previously each returned [] or None, so a typo read as "that branch
        is empty" rather than as a mistake. MATLAB raises
        DID:Database:InvalidBranch."""
        for name, call in [
            ("get_doc_ids", lambda b: self.db.get_doc_ids(b)),
            ("search", lambda b: self.db.search(self._query(), b)),
            ("get_sub_branches", lambda b: self.db.get_sub_branches(b)),
            ("get_branch_parent", lambda b: self.db.get_branch_parent(b)),
            ("remove_docs", lambda b: self.db.remove_docs("a" * 32, b)),
        ]:
            with self.subTest(method=name):
                with self.assertRaises(ValueError) as caught:
                    call("no_such_branch")
                self.assertIn("does not exist", str(caught.exception))

                with self.assertRaises(ValueError) as caught:
                    call(42)
                self.assertIn("non-empty string", str(caught.exception))

    def test_add_docs_is_the_one_that_does_not_validate_up_front(self):
        """Deliberate: MATLAB's add_docs is the only branch-taking method that
        does not call validate_branch_id, leaving the insert to refuse. It still
        raises -- just from _do_add_doc, and after the OnDuplicate check."""
        with self.assertRaises(ValueError):
            self.db.add_docs(
                [_make_doc("d" * 32)], branch_id="no_such_branch", validate=False
            )

    # -- what must NOT change ----------------------------------------------

    def test_all_doc_ids_still_spans_every_branch(self):
        """all_doc_ids() reaches _do_get_doc_ids with no argument and relies on
        the fallback that get_doc_ids no longer reaches. Validating the id
        makes that branch look dead; it is not."""
        self.assertEqual(len(self.db.all_doc_ids()), 2)
        self.assertEqual(len(self.db._do_get_doc_ids()), 2)


if __name__ == "__main__":
    unittest.main()
