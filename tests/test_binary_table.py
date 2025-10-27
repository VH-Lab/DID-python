import unittest
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from did.file.binary_table import BinaryTable
from did.file.file_obj import FileObj

class TestBinaryTable(unittest.TestCase):
    FILENAME = 'test_binary_table.bin'

    def setUp(self):
        if os.path.exists(self.FILENAME):
            os.remove(self.FILENAME)

        self.f_obj = FileObj(self.FILENAME)
        self.bt = BinaryTable(
            self.f_obj,
            ['char', 'double', 'uint64'],
            [10, 8, 8],
            [10, 1, 1],
            0
        )

    def tearDown(self):
        if os.path.exists(self.FILENAME):
            os.remove(self.FILENAME)

    def test_write_and_read_row(self):
        data = ['test1', 1.1, 100]
        self.bt.insert_row(0, data)

        read_data = self.bt.read_row(1, 1)
        self.assertEqual(read_data[0], 'test1')

        read_data = self.bt.read_row(1, 2)
        self.assertAlmostEqual(read_data[0], 1.1, places=5)

        read_data = self.bt.read_row(1, 3)
        self.assertEqual(read_data[0], 100)

    def test_write_table(self):
        data = [
            ['test1', 1.1, 100],
            ['test2', 2.2, 200],
        ]
        self.bt.write_table(data)

        read_data = self.bt.read_row(float('inf'), 1)
        self.assertEqual(read_data, ['test1', 'test2'])

if __name__ == '__main__':
    unittest.main()
