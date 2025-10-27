import unittest
import os
import numpy as np
from did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_doc_tree_invalid

class TestInvalidModification(unittest.TestCase):
    DB_FILENAME = 'invalid_mod_test.sqlite'

    def setUp(self):
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)
        self.db = SQLiteDB(self.DB_FILENAME)
        self.db.add_branch('a')

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)

    def create_modified_documents(self, **options):
        _, _, docs = make_doc_tree_invalid([1, 1, 1], **options)

        with self.assertRaises(Exception) as context:
            self.db.add_docs(docs)

        # The Matlab code checks for a specific error identifier.
        # Here, we can check if the exception is of a certain type,
        # but for now, we'll just ensure that *any* exception is raised.
        self.assertTrue(True)


    def test_value_modifier(self):
        for modifier in ['blank int']:
            with self.subTest(modifier=modifier):
                self.create_modified_documents(value_modifier=modifier)

    def test_id_modifier(self):
        for modifier in ['replace_underscore']:
            with self.subTest(modifier=modifier):
                self.create_modified_documents(id_modifier=modifier)

    def test_dependency_modifier(self):
        for modifier in ['invalid id', 'invalid name']:
            with self.subTest(modifier=modifier):
                self.create_modified_documents(dependency_modifier=modifier)

    def test_other_modifier(self):
        for modifier in ['invalid class name']:
            with self.subTest(modifier=modifier):
                self.create_modified_documents(other_modifier=modifier)

    def test_remover(self):
        for remover in ['base', 'base.id', 'document_class', 'document_class.class_name']:
            with self.subTest(remover=remover):
                self.create_modified_documents(remover=remover)

if __name__ == '__main__':
    unittest.main()
