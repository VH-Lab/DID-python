import unittest
import os
from did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_doc_tree, verify_db_document_structure

class TestDocument(unittest.TestCase):
    DB_FILENAME = 'test_db_document.sqlite'

    def setUp(self):
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)
        self.db = SQLiteDB(self.DB_FILENAME)
        self.db.add_branch('a')

    def tearDown(self):
        self.db.close_db()
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)

    def test_add_documents(self):
        g, node_names, docs = make_doc_tree([10, 10, 10])
        self.db.add_docs(docs)

        b, msg = verify_db_document_structure(self.db, g, docs)
        self.assertTrue(b, msg)

if __name__ == '__main__':
    unittest.main()
