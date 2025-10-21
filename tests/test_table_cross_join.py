import unittest
import os
from did.datastructures import table_cross_join

class TestTableCrossJoin(unittest.TestCase):
    def test_cross_join(self):
        t1 = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
        t2 = [{'c': 5, 'd': 6}, {'c': 7, 'd': 8}]

        expected_result = [
            {'a': 1, 'b': 2, 'c': 5, 'd': 6},
            {'a': 1, 'b': 2, 'c': 7, 'd': 8},
            {'a': 3, 'b': 4, 'c': 5, 'd': 6},
            {'a': 3, 'b': 4, 'c': 7, 'd': 8},
        ]

        result = table_cross_join(t1, t2)

        # The result of the list comprehension is a set, so we need to sort both lists to compare
        self.assertEqual(sorted(result, key=lambda x: (x['a'], x['c'])), sorted(expected_result, key=lambda x: (x['a'], x['c'])))

if __name__ == '__main__':
    unittest.main()