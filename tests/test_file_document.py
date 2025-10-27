import unittest
import os
import shutil
import json
from did.implementations.sqlitedb import SQLiteDB
from did.document import Document
from did.query import Query
from did.datastructures.utils import eq_len

class TestFileDocument(unittest.TestCase):
    DB_FILENAME = 'filetestdb.sqlite'

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.DB_FILENAME):
            os.remove(cls.DB_FILENAME)
        cls.db = SQLiteDB(cls.DB_FILENAME)
        cls.db.add_branch('a')

        cls.fnames = ['filename1.ext', 'filename2.ext']
        cls.fullfilenames = []
        cls.doc = Document('demoFile', **{'demoFile.value': 1})

        for i, fname in enumerate(cls.fnames):
            fullfilename = os.path.join(os.getcwd(), fname)
            cls.fullfilenames.append(fullfilename)
            with open(fullfilename, 'w') as f:
                f.write(''.join(chr(c) for c in range(i * 10, i * 10 + 10)))

            cls.doc.add_file(fname, fullfilename)

        cls.db.add_docs(cls.doc)

        for fullfilename in cls.fullfilenames:
            if os.path.exists(fullfilename):
                os.remove(fullfilename)

    @classmethod
    def tearDownClass(cls):
        cls.db.close_db()
        if os.path.exists(cls.DB_FILENAME):
            os.remove(cls.DB_FILENAME)

    def test_file_document_operations(self):
        docs = self.db.search(Query('', 'isa', 'demoFile'))
        doc_g = self.db.get_docs(docs)

        for i, fname in enumerate(self.fnames):
            f = self.db.open_doc(docs[0], fname)
            self.assertIsNotNone(f)

            data = f.read()
            f.close()

            expected_data = ''.join(chr(c) for c in range(i * 10, i * 10 + 10))
            self.assertEqual(data.decode('utf-8'), expected_data)

if __name__ == '__main__':
    unittest.main()
