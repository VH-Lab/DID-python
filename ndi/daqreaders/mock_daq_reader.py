from ..core import Channel, Probe, Epoch

class MockReader:
    """A temporary class for mocking DAQ_Reader output."""
    def __init__(self, daq_system_id, epoch_files=[]):
        self.epoch_files = epoch_files
        self.daq_system_id = daq_system_id
        self.__build_epoch_probe_map()

    def __build_epoch_probe_map(self):
        self.probes = [
            Probe('intan', 1, 'sharp_vm'),
            Probe('intan', 2, 'sharp_vm')
        ]
        self.epochs = [
            Epoch(),
            Epoch(),
            Epoch()
        ]
        self.channels = [
            Channel('ai1', 1, 'analog_in', 't1/test.data', self.epochs[0].id, self.probes[0].id ),
            Channel('di2', 2, 'digital_in', 't2/test.data', self.epochs[1].id, self.probes[0].id),
            Channel('aux3', 3, 'auxiliary', 't3/test.data', self.epochs[2].id, self.probes[1].id)
        ]

    def add_daq_system_ids(self, items):
        def add_id(item):
            item.daq_system_id = self.daq_system_id
            return item
        return list(map(add_id, items))

    def get_probes(self):
        return self.add_daq_system_ids(self.probes)

    def get_epochs(self):
        return self.add_daq_system_ids(self.epochs)

    def get_channels(self):
        return self.add_daq_system_ids(self.channels)


class EmptyMockReader:
    """A temporary class for mocking DAQ_Reader output."""
    def __init__(self, daq_system_id, epoch_files=[]):
        self.epoch_files = epoch_files
        self.daq_system_id = daq_system_id
        self.__build_epoch_probe_map()

    def __build_epoch_probe_map(self):
        self.probes = []
        self.epochs = []
        self.channels = []
        
    def add_daq_system_ids(self, items):
        def add_id(item):
            item.daq_system_id = self.daq_system_id
            return item
        return list(map(add_id, items))

    def get_probes(self):
        return self.add_daq_system_ids(self.probes)

    def get_epochs(self):
        return self.add_daq_system_ids(self.epochs)

    def get_channels(self):
        return self.add_daq_system_ids(self.channels)