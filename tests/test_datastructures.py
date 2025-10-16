import unittest
from src.did import datastructures

class TestDataStructures(unittest.TestCase):

    def test_is_full_field(self):
        A = {
            'a': {'sub1': 1, 'sub2': 2},
            'b': 5
        }
        b, value = datastructures.is_full_field(A, 'a.sub1')
        self.assertTrue(b)
        self.assertEqual(value, 1)

        b, value = datastructures.is_full_field(A, 'a.sub3')
        self.assertFalse(b)
        self.assertIsNone(value)

    def test_struct_merge(self):
        s1 = {'a': 1, 'b': 2, 'c': 3}
        s2 = {'a': 11, 'b': 12}
        S = datastructures.struct_merge(s1, s2)
        self.assertEqual(S['a'], 11)
        self.assertEqual(S['b'], 12)
        self.assertEqual(S['c'], 3)

        s1 = {'a': 1, 'b': 2, 'c': 3}
        s2 = {'a': 11, 'b': 12, 'd': 4}
        S = datastructures.struct_merge(s1, s2)
        self.assertEqual(S['d'], 4)

        with self.assertRaises(ValueError):
            datastructures.struct_merge(s1, s2, error_if_new_field=True)

    def test_struct_merge_alphabetical(self):
        s1 = {'b': 2, 'c': 3, 'a': 1}
        s2 = {'b': 12, 'd': 4, 'a': 11}
        S = datastructures.struct_merge(s1, s2, do_alphabetical=True)
        self.assertEqual(list(S.keys()), ['a', 'b', 'c', 'd'])

    def test_struct_merge_with_empty(self):
        s1 = {}
        s2 = {'b': 12, 'd': 4, 'a': 11}
        S = datastructures.struct_merge(s1, s2, do_alphabetical=True)
        self.assertEqual(list(S.keys()), ['a', 'b', 'd'])

if __name__ == '__main__':
    unittest.main()