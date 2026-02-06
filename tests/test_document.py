import unittest
import os
from did.document import Document

class TestDocument(unittest.TestCase):
    def setUp(self):
        # Set the schema path for the document class
        self.schema_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'did', 'example_schema', 'demo_schema1')
        Document.set_schema_path(self.schema_path)

    def test_dependency_management(self):
        # Create a document of type 'demoC', which has 'depends_on' fields
        doc = Document('demoC')

        # Verify 'depends_on' field exists
        self.assertIn('depends_on', doc.document_properties,
                      "The 'depends_on' field should exist for 'demoC' document type.")

        # Test setting a new dependency value
        doc.set_dependency_value('item1', 'new_value')
        retrieved_value = doc.dependency_value('item1')
        self.assertEqual(retrieved_value, 'new_value',
                         "Failed to set and retrieve a new dependency value.")

        # Test updating an existing dependency value
        doc.set_dependency_value('item1', 'updated_value')
        retrieved_value = doc.dependency_value('item1')
        self.assertEqual(retrieved_value, 'updated_value',
                         "Failed to update an existing dependency value.")

    def test_file_management(self):
        # Create a document of type 'demoFile', which is defined to handle files
        doc = Document('demoFile')

        # Add a file and verify it was added
        doc.add_file('filename1.ext', '/path/to/file1.txt')
        is_in, _, fI_index = doc.is_in_file_list('filename1.ext')
        self.assertTrue(is_in, "File 'filename1.ext' should be in the file list.")
        self.assertIsNotNone(fI_index, "File info index should not be empty after adding a file.")

        # Verify the location of the added file
        self.assertEqual(doc.document_properties['files']['file_info'][fI_index]['locations']['location'], '/path/to/file1.txt',
                         "The location of the added file is incorrect.")

        # Remove the file and verify it was removed
        doc.remove_file('filename1.ext')
        is_in_after_removal, _, fI_index_after_removal = doc.is_in_file_list('filename1.ext')
        # After removal, searching for the file info should yield an empty index
        self.assertFalse(is_in_after_removal)
        self.assertIsNone(fI_index_after_removal, "File info should be empty after removing the file.")

if __name__ == '__main__':
    unittest.main()