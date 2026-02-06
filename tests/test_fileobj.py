import unittest
from did.file import Fileobj

class TestFileobj(unittest.TestCase):
    def test_constructor(self):
        # Test creating a fileobj
        the_file_obj = Fileobj()
        self.assertIsNone(the_file_obj.fid)
        self.assertEqual(the_file_obj.permission, 'r')
        self.assertEqual(the_file_obj.machineformat, 'n')
        self.assertEqual(the_file_obj.fullpathfilename, '')

    def test_custom_file_handler_error(self):
        # Test that passing customFileHandler to the constructor throws an error
        def my_handler(x):
            print(f'File operation: {x}')

        with self.assertRaises(TypeError):
            Fileobj(customFileHandler=my_handler)

if __name__ == '__main__':
    unittest.main()