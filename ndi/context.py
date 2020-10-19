from __future__ import annotations
import ndi.types as T

class Context:
    def __init__(
        self, 
        raw_data_directory: str = '',
        database: T.NdiDatabase = None, 
        binary_collection: T.BinaryCollection = None,
        daq_systems: T.List[T.DaqSystem] = [],
        daq_readers_map: T.Dict[str, T.DaqReader] = {}
    ):
        self.raw_data_directory = raw_data_directory
        self.database = database
        self.binary_collection = binary_collection
        self.daq_systems = daq_systems
        self.daq_readers_map = daq_readers_map
    
    @property
    def dir(self):
        return self.raw_data_directory
    
    @property
    def db(self):
        return self.database

    @property
    def bin(self):
        return self.binary_collection

    def load_daq_system(self, daq_system):
        if daq_system.id not in [ds.id for ds in self.daq_systems]:
            self.daq_systems.append(daq_system)
        self._load_daq_reader(daq_system.daq_reader)

    def _load_daq_reader(self, daq_reader):
        """Loads a DaqReader to the context. Should be handled by load_daq_system."""
        reader_name = daq_reader.__name__
        if reader_name not in self.daq_readers_map:
            self.daq_readers_map[reader_name] = daq_reader
