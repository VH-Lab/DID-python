import pytest
from ndi import FileNavigator, Document

@pytest.fixture
def new_file_navigator():
    epoch_file_patterns = ['.*\.smr$', '.*\.epochmetadata$'],
    metadata_file_pattern = '.*\.epochmetadata$'
    fn = FileNavigator(epoch_file_patterns, metadata_file_pattern)
    return fn, epoch_file_patterns, metadata_file_pattern

class TestFileNavigator:
    def test_new_file_navigator(self, new_file_navigator):
        fn, epoch_file_patterns, metadata_file_pattern = new_file_navigator

        # Test properties in document data match what was passed in at instantiation
        assert fn.document.data['_metadata']['type'] == FileNavigator.DOCUMENT_TYPE
        assert fn.document.data['epoch_file_patterns'] == epoch_file_patterns
        assert fn.document.data['metadata_file_pattern'] == metadata_file_pattern

    def test_document_to_file_navigator(self, new_file_navigator):
        fn, _, _ = new_file_navigator

        fn_doc = fn.document
        rebuilt_fn = FileNavigator.from_document(fn_doc)

        # Test that FileNavigator object can be recreated from corresponding document
        assert rebuilt_fn == fn
