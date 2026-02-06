import unittest
from did.datastructures import *

class TestDataStructures(unittest.TestCase):
    def test_cell_to_str(self):
        self.assertEqual(cell_to_str(['a', 'b', 'c']), '["a", "b", "c"]')
        self.assertEqual(cell_to_str([]), '[]')

    def test_cell_or_item(self):
        self.assertEqual(cell_or_item(['a', 'b', 'c'], 1), 'b')
        self.assertEqual(cell_or_item('a'), 'a')

    def test_col_vec(self):
        self.assertEqual(col_vec([1, 2, 3]), [1, 2, 3])
        self.assertEqual(col_vec([[1, 2], [3, 4]]), [1, 3, 2, 4])

    def test_empty_struct(self):
        self.assertEqual(empty_struct('a', 'b'), {})

    def test_is_empty(self):
        self.assertTrue(is_empty(None))
        self.assertTrue(is_empty([]))
        self.assertFalse(is_empty([1]))
        self.assertFalse(is_empty(0))

    def test_eq_emp(self):
        self.assertTrue(eq_emp(None, []))
        self.assertFalse(eq_emp(None, [1]))
        self.assertTrue(eq_emp([1], [1]))
        self.assertFalse(eq_emp([1], [2]))

    def test_size_eq(self):
        self.assertTrue(size_eq([1, 2], [3, 4]))
        self.assertFalse(size_eq([1, 2], [1, 2, 3]))

    def test_eq_tot(self):
        self.assertTrue(eq_tot([1, 2], [1, 2]))
        self.assertFalse(eq_tot([1, 2], [1, 3]))

    def test_eq_len(self):
        self.assertTrue(eq_len([1, 2], [1, 2]))
        self.assertFalse(eq_len([1, 2], [1, 3]))
        self.assertFalse(eq_len([1, 2], [1, 2, 3]))

    def test_eq_unique(self):
        self.assertEqual(eq_unique([[1, 2], [1, 2], [1, 3]]), [[1, 2], [1, 3]])

    def test_is_full_field(self):
        d = {'a': {'b': {'c': 1}}}
        self.assertTrue(is_full_field(d, 'a.b.c')[0])
        self.assertFalse(is_full_field(d, 'a.b.d')[0])

    def test_struct_partial_match(self):
        a = {'a': 1, 'b': 2}
        b = {'a': 1}
        c = {'a': 2}
        self.assertTrue(struct_partial_match(a, b))
        self.assertFalse(struct_partial_match(a, c))

    def test_field_search(self):
        a = {'a': 1, 'b': 'hello'}
        search_struct = [{'field': 'a', 'operation': 'exact_number', 'param1': 1}]
        self.assertTrue(field_search(a, search_struct))
        search_struct = [{'field': 'b', 'operation': 'contains_string', 'param1': 'ell'}]
        self.assertTrue(field_search(a, search_struct))

    def test_find_closest(self):
        arr = [1, 5, 10, 15]
        self.assertEqual(find_closest(arr, 6), (1, 5))
        self.assertEqual(find_closest(arr, 14), (3, 15))

    def test_json_encode_nan(self):
        d = {'a': 1, 'b': float('nan')}
        self.assertIn('NaN', json_encode_nan(d))

    def test_struct_merge(self):
        s1 = {'a': 1, 'b': 2}
        s2 = {'b': 3, 'c': 4}
        self.assertEqual(struct_merge(s1, s2), {'a': 1, 'b': 3, 'c': 4})
        with self.assertRaises(ValueError):
            struct_merge(s1, s2, error_if_new_field=True)

if __name__ == '__main__':
    unittest.main()