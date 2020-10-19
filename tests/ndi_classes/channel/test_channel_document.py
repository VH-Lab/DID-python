import pytest
from ndi import Channel, Document

class MockDaqReader:
    def __init__(self, file):
        pass

@pytest.fixture
def new_channel():
    name = 'abc'
    number = 1
    type_ = 'best channel'
    source_file = 'file/path'
    epoch_id = 'qwertyuiop'
    probe_id = 'poiuytrewq'
    daq_system_id = '0987654321'
    experiment_id = '1234567890'
    clock_type = 'no_time'
    c = Channel(
        name=name,
        number=number,
        type_=type_,
        source_file=source_file,
        epoch_id=epoch_id,
        probe_id=probe_id,
        daq_reader=MockDaqReader,
        daq_system_id=daq_system_id,
        experiment_id=experiment_id,
        clock_type=clock_type
    )
    yield c, name, number, type_, source_file, epoch_id, probe_id, daq_system_id, experiment_id, clock_type

class TestChannelDocument:
    def test_new_channel(self, new_channel):
        """ndi.Channel.__init__"""
        c, name, number, type_, source_file, epoch_id, probe_id, daq_system_id, experiment_id, clock_type = new_channel

        # metadata is properly set in document
        assert c.document.data['_metadata']['name'] == name
        assert c.document.data['_metadata']['type'] == Channel.DOCUMENT_TYPE
        assert c.document.data['_metadata']['experiment_id'] == experiment_id

        # data is properly set in document
        assert c.document.data['number'] == number
        assert c.document.data['type'] == type_
        assert c.document.data['source_file'] == source_file
        assert c.document.data['epoch_id'] == epoch_id
        assert c.document.data['probe_id'] == probe_id
        assert c.document.data['daq_system_id'] == daq_system_id

    def test_document_to_channel(self, new_channel):
        """ndi.Channel.from_document"""
        c, name, number, type_, source_file, epoch_id, probe_id, daq_system_id, experiment_id, clock_type = new_channel

        d = c.document
        rebuilt_channel = Channel.from_document(d)
        assert rebuilt_channel == c
