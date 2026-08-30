import os
import unittest

from did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_doc_tree


class TestValidModification(unittest.TestCase):
    DB_FILENAME = "test_valid_modification.sqlite"

    def setUp(self):
        # Create a temporary database for testing
        self.db_path = os.path.join(os.path.dirname(__file__), self.DB_FILENAME)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = SQLiteDB(self.db_path)
        self.db.add_branch("a")
        # Ensure at least one document is created
        _, _, self.docs = make_doc_tree([1, 1, 1])
        while not self.docs:
            _, _, self.docs = make_doc_tree([1, 1, 1])

        for doc in self.docs:
            self.db._do_add_doc(doc, "a")

    def tearDown(self):
        # Clean up the database file
        self.db._close_db()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_removed_doc_id_cannot_be_readded(self):
        """A document removed from its last branch is gone for good.

        This test previously asserted the opposite -- that the same id could
        be added back. DID-matlab issue #55 retires the id instead, and this
        is the deliberate behavior change that came with porting it.

        The reason is not that the old re-add was broken here. Python has
        reclaimed the doc_data rows since #39, so the re-added document got
        fresh rows rather than the stale ones MATLAB used to resurrect. The
        reason is that ids must not be re-used under DID's branch model, and
        that both languages must agree about whether this operation is legal
        on the same database file.
        """
        doc = self.docs[0]
        doc_id = doc.id()

        # Remove the document
        self.db.remove_docs(doc_id, "a")

        # Verify it's gone
        retrieved_doc = self.db.get_docs(doc_id, OnMissing="ignore")
        self.assertIsNone(retrieved_doc)

        # Re-adding the retired id is refused
        with self.assertRaises(ValueError) as caught:
            self.db._do_add_doc(doc, "a")
        self.assertIn("previously removed", str(caught.exception))

        # ...and it stayed gone
        self.assertIsNone(self.db.get_docs(doc_id, OnMissing="ignore"))


if __name__ == "__main__":
    unittest.main()
