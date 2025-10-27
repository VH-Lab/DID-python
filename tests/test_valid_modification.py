import unittest
import os
from did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_doc_tree_invalid

class TestValidModification(unittest.TestCase):
    DB_FILENAME = 'valid_mod_test.sqlite'

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
        """
        Creates modified documents and adds them to the database.
        This test expects NO exception to be raised.
        """
        _, _, docs = make_doc_tree_invalid([1, 1, 1], **options)
        try:
            self.db.add_docs(docs)
        except Exception as e:
            self.fail(f"add_docs raised an unexpected exception for options {options}: {e}")

    def test_sham_modification(self):
        """
        Tests that a 'sham' modification (i.e., no modification)
        completes successfully.
        """
        self.create_modified_documents()

    def test_remover(self):
        """
        Tests that removing fields with default values is a valid modification.
        """
        for remover in ['definition', 'validation']:
            with self.subTest(remover=remover):
                self.create_modified_documents(remover=remover)

    def test_dependency_modifier(self):
        """
        Tests that adding a dependency is a valid modification.
        """
        for modifier in ['add dependency']:
            with self.subTest(modifier=modifier):
                self.create_modified_documents(dependency_modifier=modifier)

    def test_other_modifier(self):
        """
        Tests other valid modifications. The names are misleading but are
        treated as valid in this context.
        """
        for modifier in [
            'invalid definition',
            'invalid property list name',
            'new class version number',
            'class version string',
            'invalid base name'
        ]:
            with self.subTest(modifier=modifier):
                self.create_modified_documents(other_modifier=modifier)


if __name__ == '__main__':
    unittest.main()
