import unittest
import os
from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from .helpers import make_doc_tree

class TestValidModification(unittest.TestCase):
    DB_FILENAME = 'test_valid_modification.sqlite'

    def setUp(self):
        # Create a temporary database for testing
        self.db_path = os.path.join(os.path.dirname(__file__), self.DB_FILENAME)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = SQLiteDB(self.db_path)
        self.db.add_branch('a')
        # Ensure at least one document is created
        _, _, self.docs = make_doc_tree([1, 1, 1])
        while not self.docs:
            _, _, self.docs = make_doc_tree([1, 1, 1])

        for doc in self.docs:
            self.db._do_add_doc(doc, 'a')

    def tearDown(self):
        # Clean up the database file
        self.db._close_db()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_remove_and_readd_doc(self):
        doc = self.docs[0]
        doc_id = doc.id()

        # Remove the document
        self.db.remove_docs(doc_id, 'a')

        # Verify it's gone
        retrieved_doc = self.db.get_docs(doc_id, OnMissing='ignore')
        self.assertIsNone(retrieved_doc)

        # Re-add the document
        self.db._do_add_doc(doc, 'a')

        # Verify it's back
        retrieved_doc = self.db.get_docs(doc_id)
        self.assertIsNotNone(retrieved_doc)
        self.assertEqual(retrieved_doc.id(), doc_id)

if __name__ == '__main__':
    unittest.main()