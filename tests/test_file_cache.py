import unittest
import os
import shutil
from did.file.file_cache import FileCache

class TestFileCache(unittest.TestCase):
    CACHE_DIR = 'test_cache'
    FILENAME = 'test_file.txt'

    def setUp(self):
        if os.path.exists(self.CACHE_DIR):
            shutil.rmtree(self.CACHE_DIR)
        os.makedirs(self.CACHE_DIR)

        with open(self.FILENAME, 'w') as f:
            f.write('test data')

        self.fc = FileCache(self.CACHE_DIR)

    def tearDown(self):
        if os.path.exists(self.CACHE_DIR):
            shutil.rmtree(self.CACHE_DIR)
        if os.path.exists(self.FILENAME):
            os.remove(self.FILENAME)

    def test_add_and_is_file(self):
        self.fc.add_file(self.FILENAME, 'test_file.txt')
        self.assertTrue(self.fc.is_file('test_file.txt'))

        file_list = self.fc.file_list()
        self.assertEqual(len(file_list), 1)
        self.assertEqual(file_list[0]['name'], 'test_file.txt')

    def test_resize(self):
        self.fc.max_size = 100
        self.fc.reduce_size = 80

        # Add a file that is larger than the reduce_size
        with open('large_file.txt', 'w') as f:
            f.write('a' * 90)
        self.fc.add_file('large_file.txt', 'large_file.txt')

        # Now add another file, which should trigger a resize
        with open('small_file.txt', 'w') as f:
            f.write('a' * 20)
        self.fc.add_file('small_file.txt', 'small_file.txt')

        # The large file should have been removed
        self.assertFalse(self.fc.is_file('large_file.txt'))
        self.assertTrue(self.fc.is_file('small_file.txt'))

if __name__ == '__main__':
    unittest.main()
