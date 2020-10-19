import pytest
from ndi import Epoch, Document

@pytest.fixture
def new_epoch():
    experiment_id = '1234567890'
    e = Epoch(experiment_id=experiment_id)
    yield e, experiment_id

class TestEpochDocument:
    def test_new_epoch(self, new_epoch):
        """ndi.Epoch.__init__"""
        e, experiment_id = new_epoch

        # metadata is properly set in document
        assert e.document.data['_metadata']['type'] == Epoch.DOCUMENT_TYPE
        assert e.document.data['_metadata']['experiment_id'] == experiment_id

    def test_document_to_epoch(self, new_epoch):
        """ndi.Epoch.from_document"""
        e, experiment_id = new_epoch

        d = e.document
        rebuilt_epoch = Epoch.from_document(d)
        assert rebuilt_epoch == e
