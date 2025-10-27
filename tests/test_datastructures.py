import unittest
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from did.datastructures.utils import is_full_field
from did.datastructures.struct_merge import struct_merge

class TestDatastructures(unittest.TestCase):
    def test_is_full_field(self):
        a = {
            'a': {'sub1': 1, 'sub2': 2},
            'b': 5
        }

        b, value = is_full_field(a, 'a.sub1')
        self.assertTrue(b)
        self.assertEqual(value, 1)

        b, value = is_full_field(a, 'a.sub3')
        self.assertFalse(b)
        self.assertIsNone(value)

    def test_struct_merge(self):
        s1 = {'a': 1, 'b': 2, 'c': 3}
        s2 = {'a': 11, 'b': 12}

        s = struct_merge(s1, s2)
        self.assertEqual(s['a'], 11)
        self.assertEqual(s['b'], 12)
        self.assertEqual(s['c'], 3)

        s1 = {'a': 1, 'b': 2, 'c': 3}
        s2 = {'a': 11, 'b': 12, 'd': 4}

        s = struct_merge(s1, s2)
        self.assertEqual(s['d'], 4)

        with self.assertRaises(ValueError):
            struct_merge(s1, s2, error_if_new_field=True)

    def test_struct_merge_alphabetical(self):
        s1 = {'b': 2, 'c': 3, 'a': 1}
        s2 = {'b': 12, 'd': 4, 'a': 11}

        s = struct_merge(s1, s2, do_alphabetical=True)
        self.assertEqual(list(s.keys()), ['a', 'b', 'c', 'd'])

    def test_struct_merge_with_empty(self):
        s1 = {}
        s2 = {'b': 12, 'd': 4, 'a': 11}

        s = struct_merge(s1, s2, do_alphabetical=True)
        self.assertEqual(list(s.keys()), ['a', 'b', 'd'])

if __name__ == '__main__':
    unittest.main()
