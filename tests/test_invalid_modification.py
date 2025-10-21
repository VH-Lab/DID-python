import unittest
import os
from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from .helpers import make_doc_tree

class TestInvalidModification(unittest.TestCase):
    DB_FILENAME = 'test_invalid_modification.sqlite'

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

    def test_add_doc_twice(self):
        # Adding the same document twice should not raise an error
        doc = self.docs[0]
        try:
            self.db._do_add_doc(doc, 'a')
        except Exception as e:
            self.fail(f"Adding the same document twice raised an exception: {e}")

    def test_add_doc_to_nonexistent_branch(self):
        doc = self.docs[0]
        with self.assertRaises(ValueError):
            self.db._do_add_doc(doc, 'nonexistent_branch')

    def test_remove_doc_from_nonexistent_branch(self):
        doc_id = self.docs[0].id()
        with self.assertRaises(ValueError):
            self.db._do_remove_doc(doc_id, 'nonexistent_branch')

    def test_get_doc_from_nonexistent_branch(self):
        doc_id = self.docs[0].id()
        # This should not raise an error, but should return None
        result = self.db.get_docs(doc_id, branch_id='nonexistent_branch')
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()