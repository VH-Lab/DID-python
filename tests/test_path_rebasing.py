import unittest
import os
import shutil
from did.document import Document
from did.file import ReadOnlyFileobj
from did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_doc_tree

class TestPathRebasing(unittest.TestCase):
    DB_FILENAME = 'test_path_rebasing.sqlite'
    SUBDIR = 'subdir_for_test'

    def setUp(self):
        # Create a subdirectory for the database
        if os.path.exists(self.SUBDIR):
            shutil.rmtree(self.SUBDIR)
        os.makedirs(self.SUBDIR)

        self.db_path = os.path.join(self.SUBDIR, self.DB_FILENAME)
        self.db = SQLiteDB(self.db_path)
        self.db.add_branch('a')

        # Create a file inside the subdirectory (relative to DB)
        self.relative_filename = 'relative_file.txt'
        self.file_content = 'Content of relative file.'
        with open(os.path.join(self.SUBDIR, self.relative_filename), 'w') as f:
            f.write(self.file_content)

    def tearDown(self):
        self.db._close_db()
        if os.path.exists(self.SUBDIR):
            shutil.rmtree(self.SUBDIR)

    def test_open_doc_relative_path(self):
        # Create a document and add the file using a relative path
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'did', 'example_schema', 'demo_schema1')
        Document.set_schema_path(schema_path)
        doc = Document('demoFile')

        # Add file with just the filename, which implies it's in the same dir as DB
        doc.add_file('my_file', self.relative_filename)
        self.db._do_add_doc(doc, 'a')

        # Open the document and retrieve the file
        # The logic should resolve self.relative_filename relative to self.db_path
        file_obj = self.db.open_doc(doc.id(), 'my_file')

        self.assertIsInstance(file_obj, ReadOnlyFileobj)
        # Check content to ensure correct file was opened
        # Note: ReadOnlyFileobj defaults to binary mode 'rb' unless specified otherwise in its implementation wrapper or here?
        # Let's check how ReadOnlyFileobj behaves. It inherits from Fileobj which defaults to 'rb' if 'b' not in permission but adds it.
        # But ReadOnlyFileobj sets permission='r'. Fileobj.fopen adds 'b' if not present. So it opens as 'rb'.

        file_obj.fopen()
        content = file_obj.fread().decode('utf-8')
        file_obj.fclose()

        self.assertEqual(content, self.file_content)

if __name__ == '__main__':
    unittest.main()
