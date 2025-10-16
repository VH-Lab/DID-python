import unittest
import os
from src.did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_doc_tree, verify_db_document_structure
from src.did.document import Document

class TestDocument(unittest.TestCase):
    DB_FILENAME = 'test_db_docs.sqlite'

    def setUp(self):
        # Create a temporary working directory to run tests in
        self.db = SQLiteDB(self.DB_FILENAME)
        self.db.add_branch('a')

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)

    def test_add_documents(self):
        g, node_names, docs = make_doc_tree([10, 10, 10])

        # In the Python version, we don't need to plot the graph for testing

        self.db.add_docs(docs)

        b, msg = verify_db_document_structure(self.db, g, docs)
        self.assertTrue(b, msg)

    def test_remove_documents(self):
        g, node_names, docs = make_doc_tree([30, 30, 30])
        self.db.add_docs(docs)
        b, msg = verify_db_document_structure(self.db, g, docs)
        self.assertTrue(b, msg)

        # Simplified version of the removal test
        # A full port would require porting rm_doc_tree and add_doc_tree as well

        docs_to_delete = docs[:5]
        doc_ids_to_delete = [doc.id() for doc in docs_to_delete]

        self.db.remove_docs(doc_ids_to_delete)

        remaining_docs = docs[5:]

        # Verify that the remaining docs are still in the database
        b, msg = verify_db_document_structure(self.db, g, remaining_docs)
        self.assertTrue(b, msg)

        # Verify that the deleted docs are no longer in the database
        for doc_id in doc_ids_to_delete:
            doc = self.db.get_docs(doc_id, OnMissing='ignore')
            self.assertIsNone(doc, f"Document {doc_id} should have been deleted.")

        # Verify that calling get_docs with 'error' raises an error for a deleted doc
        with self.assertRaises(ValueError):
            self.db.get_docs(doc_ids_to_delete[0])

if __name__ == '__main__':
    unittest.main()