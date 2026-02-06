import unittest
import os
import random
from did.implementations.sqlitedb import SQLiteDB
from did.query import Query
from .helpers import make_doc_tree, verify_db_document_structure, get_demo_type, apply_did_query

class TestDbQueries(unittest.TestCase):
    DB_FILENAME = 'test_db_queries.sqlite'
    db = None
    docs = None

    @classmethod
    def setUpClass(cls):
        # Ensure the database file is removed before starting
        if os.path.exists(cls.DB_FILENAME):
            os.remove(cls.DB_FILENAME)

        cls.db = SQLiteDB(cls.DB_FILENAME)
        cls.db.add_branch('a')
        _, _, cls.docs = make_doc_tree([10, 10, 10])
        for doc in cls.docs:
            cls.db._do_add_doc(doc, 'a')

    @classmethod
    def tearDownClass(cls):
        cls.db._close_db()
        if os.path.exists(cls.DB_FILENAME):
            os.remove(cls.DB_FILENAME)

    def _test_query(self, q):
        ids_actual = self.db.search(q, branch_id='a')
        self.assertIsInstance(ids_actual, list)

        ids_expected, _ = apply_did_query(self.docs, q)

        self.assertEqual(sorted(ids_actual), sorted(ids_expected))

    def get_random_document_id(self):
        return random.choice(self.docs).id()

    def test_exact_string(self):
        id_chosen = self.get_random_document_id()
        q = Query('base.id', 'exact_string', id_chosen)
        self._test_query(q)

    def test_not_exact_string(self):
        id_chosen = self.get_random_document_id()
        q = Query('base.id', '~exact_string', id_chosen)
        self._test_query(q)

    def test_and(self):
        doc_id = self.docs[0].id()
        demo_type = get_demo_type(self.docs[0])
        field_name = f"{demo_type}.value"
        q = Query('base.id', 'exact_string', doc_id) & Query(field_name, 'exact_number', 1)
        self._test_query(q)

    def test_or(self):
        doc1 = self.docs[0]
        doc2 = self.docs[1]
        demo_type1 = get_demo_type(doc1)
        demo_type2 = get_demo_type(doc2)
        field_name1 = f"{demo_type1}.value"
        field_name2 = f"{demo_type2}.value"
        q = Query(field_name1, 'exact_number', 1) | Query(field_name2, 'exact_number', 2)
        self._test_query(q)

    def test_contains_string(self):
        id_chosen = self.get_random_document_id()
        sub_string = id_chosen[10:12]
        q = Query('base.id', 'contains_string', sub_string)
        self._test_query(q)

    def test_do_not_contains_string(self):
        id_chosen = self.get_random_document_id()
        sub_string = id_chosen[10:12]
        q = Query('base.id', '~contains_string', sub_string)
        self._test_query(q)

    def test_less_than(self):
        number_chosen = random.randint(1, 100)
        q = Query('demoA.value', 'lessthan', number_chosen)
        self._test_query(q)

    def test_less_than_equal(self):
        number_chosen = 48
        q = Query('demoA.value', 'lessthaneq', number_chosen)
        self._test_query(q)

    def test_do_greater_than(self):
        number_chosen = 1
        q = Query('demoA.value', 'greaterthan', number_chosen)
        self._test_query(q)

    def test_do_greater_than_equal(self):
        number_chosen = 1
        q = Query('demoA.value', 'greaterthaneq', number_chosen)
        self._test_query(q)

    def test_has_field(self):
        q = Query('demoA.value', 'hasfield')
        self._test_query(q)

    def test_has_member(self):
        q = Query('demoA.value', 'hasmember', 1)
        self._test_query(q)

    def test_depends_on(self):
        # Find a doc with a dependency
        doc_with_dep = None
        for doc in self.docs:
            if 'depends_on' in doc.document_properties and doc.document_properties['depends_on']:
                doc_with_dep = doc
                break

        if doc_with_dep:
            dep = doc_with_dep.document_properties['depends_on'][0]
            q = Query('', 'depends_on', dep['name'], dep['value'])
            self._test_query(q)

    def test_do_is_a(self):
        q = Query('', 'isa', 'demoB')
        self._test_query(q)

    def test_do_reg_exp(self):
        q = Query('base.datestamp', 'regexp', r'\d{4}-\d{2}-\d{2}')
        self._test_query(q)

if __name__ == '__main__':
    unittest.main()