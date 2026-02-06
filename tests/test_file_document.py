import unittest
import os
from did.document import Document
from did.file import ReadOnlyFileobj
from did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_doc_tree

class TestFileDocument(unittest.TestCase):
    DB_FILENAME = 'test_file_document.sqlite'

    def setUp(self):
        # Create a temporary database for testing
        self.db_path = os.path.join(os.path.dirname(__file__), self.DB_FILENAME)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = SQLiteDB(self.db_path)
        self.db.add_branch('a')
        _, _, self.docs = make_doc_tree([1, 1, 1])
        for doc in self.docs:
            self.db._do_add_doc(doc, 'a')

    def tearDown(self):
        # Clean up the database file
        self.db._close_db()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_add_and_open_file(self):
        # Create a document of type 'demoFile'
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'did', 'example_schema', 'demo_schema1')
        Document.set_schema_path(schema_path)
        doc = Document('demoFile')

        # Create a dummy file to add
        dummy_file_path = 'dummy_file.txt'
        with open(dummy_file_path, 'w') as f:
            f.write('This is a test file.')

        # Add the file to the document
        doc.add_file('test_file.txt', dummy_file_path)

        # Add the document to the database
        self.db._do_add_doc(doc, 'a')

        # Open the file from the document
        file_obj = self.db.open_doc(doc.id(), 'test_file.txt')
        self.assertIsInstance(file_obj, ReadOnlyFileobj)

        # Clean up the dummy file
        os.remove(dummy_file_path)

if __name__ == '__main__':
    unittest.main()