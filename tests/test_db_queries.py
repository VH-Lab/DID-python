import unittest
import os
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query
from tests.helpers import make_doc_tree, verify_db_document_structure
from tests.utility_helpers import apply_did_query

class TestDbQueries(unittest.TestCase):
    DB_FILENAME = 'test_db_queries.sqlite'

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

    def _test_query(self, query):
        ids_actual = self.db.search(query)
        self.assertIsInstance(ids_actual, list)

        ids_expected, _ = apply_did_query(self.docs, query)

        self.assertEqual(sorted(ids_actual), sorted(ids_expected))

    def test_exact_string(self):
        doc_id = self.docs[0].id()
        q = Query('base.id', 'exact_string', doc_id)
        self._test_query(q)

    def test_not_exact_string(self):
        doc_id = self.docs[0].id()
        q = Query('base.id', '~exact_string', doc_id)
        self._test_query(q)

if __name__ == '__main__':
    unittest.main()
