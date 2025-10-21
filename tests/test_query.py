import unittest
from did.query import Query

class TestQuery(unittest.TestCase):
    def test_creation(self):
        # Test creating a query
        q = Query('base.name', 'exact_string', 'myname')
        ss = q.to_search_structure()
        self.assertEqual(ss[0]['field'], 'base.name')
        self.assertEqual(ss[0]['operation'], 'exact_string')
        self.assertEqual(ss[0]['param1'], 'myname')

    def test_invalid_operator(self):
        # Test that an invalid operator throws an error
        with self.assertRaises(ValueError):
            Query('base.name', 'invalid_op', 'myname')

    def test_and_query(self):
        # Test combining queries with AND
        q1 = Query('base.name', 'exact_string', 'myname')
        q2 = Query('base.age', 'greaterthan', 30)
        q_and = q1 & q2
        ss = q_and.to_search_structure()
        self.assertEqual(len(ss), 2)
        self.assertEqual(ss[0]['field'], 'base.name')
        self.assertEqual(ss[0]['operation'], 'exact_string')
        self.assertEqual(ss[0]['param1'], 'myname')
        self.assertEqual(ss[1]['field'], 'base.age')
        self.assertEqual(ss[1]['operation'], 'greaterthan')
        self.assertEqual(ss[1]['param1'], 30)

    def test_or_query(self):
        # Test combining queries with OR
        q1 = Query('base.name', 'exact_string', 'myname')
        q2 = Query('base.age', 'greaterthan', 30)
        q_or = q1 | q2
        ss = q_or.to_search_structure()
        self.assertEqual(ss[0]['operation'], 'or')

        # The parameters of the OR query are themselves search structures
        param1 = ss[0]['param1']
        self.assertEqual(param1[0]['field'], 'base.name')
        self.assertEqual(param1[0]['operation'], 'exact_string')
        self.assertEqual(param1[0]['param1'], 'myname')

        param2 = ss[0]['param2']
        self.assertEqual(param2[0]['field'], 'base.age')
        self.assertEqual(param2[0]['operation'], 'greaterthan')
        self.assertEqual(param2[0]['param1'], 30)

if __name__ == '__main__':
    unittest.main()