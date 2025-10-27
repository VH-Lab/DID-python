import unittest
import os
from did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_doc_tree, verify_db_document_structure

class TestSimpleBranch(unittest.TestCase):
    DB_FILENAME = 'test_db_simple_branch.sqlite'

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.DB_FILENAME):
            os.remove(cls.DB_FILENAME)
        cls.db = SQLiteDB(cls.DB_FILENAME)
        cls.db.add_branch('a')
        cls.g, cls.node_names, cls.docs = make_doc_tree([10, 10, 10])
        cls.db.add_docs(cls.docs)

    @classmethod
    def tearDownClass(cls):
        cls.db.close_db()
        if os.path.exists(cls.DB_FILENAME):
            os.remove(cls.DB_FILENAME)

    def test_add_branch_nodes(self):
        self.db.add_branch('a_a', 'a')
        self.db.set_branch('a_a')
        b, msg = verify_db_document_structure(self.db, self.g, self.docs)
        self.assertTrue(b, msg)

        self.db.set_branch('a')
        b, msg = verify_db_document_structure(self.db, self.g, self.docs)
        self.assertTrue(b, msg)

    def test_remove_documents_from_branch_and_verify_other_branch(self):
        self.db.add_branch('a_b', 'a')
        self.db.set_branch('a_b')

        doc_ids_to_remove = [doc.id() for doc in self.docs]
        self.db.remove_docs(doc_ids_to_remove)

        self.db.set_branch('a')
        b, msg = verify_db_document_structure(self.db, self.g, self.docs)
        self.assertTrue(b, msg)

if __name__ == '__main__':
    unittest.main()
