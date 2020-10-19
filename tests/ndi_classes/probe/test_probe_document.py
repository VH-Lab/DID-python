import pytest
from ndi import Probe, Document

@pytest.fixture
def new_probe():
    name = 'abc'
    reference = 56
    type_ = 'ntrode'
    daq_system_id = '0987654321'
    experiment_id = '1234567890'
    p = Probe(name, reference, type_, daq_system_id=daq_system_id, experiment_id=experiment_id)
    yield p, name, reference, type_, daq_system_id, experiment_id

class TestProbeDocument:
    def test_new_probe(self, new_probe):
        """ndi.Probe.__init__"""
        p, name, reference, type_, daq_system_id, experiment_id = new_probe

        # metadata is properly set in document
        assert p.document.data['_metadata']['name'] == name
        assert p.document.data['_metadata']['type'] == Probe.DOCUMENT_TYPE
        assert p.document.data['_metadata']['experiment_id'] == experiment_id

        # data is properly set in document
        assert p.document.data['reference'] == reference
        assert p.document.data['type'] == type_
        assert p.document.data['daq_system_id'] == daq_system_id

    def test_document_to_probe(self, new_probe):
        """ndi.Probe.from_document"""
        p, name, reference, type_, daq_system_id, experiment_id = new_probe

        d = p.document
        rebuilt_probe = Probe.from_document(d)
        assert rebuilt_probe == p
