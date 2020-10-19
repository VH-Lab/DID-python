import re
import numpy as np
import os
import xml.etree.ElementTree as ET
from ndi.daqsystem.mfdaq import DaqReaderMultiFunction
from ndi.daqsystem.probe import Probe
from ndi.schema.ChannelType import ChannelType


class ProbeNTrode(Probe):
    """SpikeGadgets NTrode channel"""

    def __init__(self, name, reference):
        self.name = name
        self.reference = reference
        # Auxiliary
        if name[0] == 'A':
            self.type = ChannelType.auxiliary
            if name[1] == 'i':
                self.number = int(name[3:])
            else:
                self.number = int(name[4:])
        elif name[0] == 'D':
            if name[1] == 'i':
                self.type = ChannelType.digital_in
                self.number = int(name[3:])
            else:
                self.type = ChannelType.digital_out
                self.number = int(name[4:])
        elif name.startswith('Tetrode'):
            self.type = ChannelType.analog_in
            self.number = int(name[7:])
        else:
            # MCU (digital inputs)
            self.type = ChannelType.digital_in
            self.number = int(name[7:]) + 32


class DaqReaderMultiFunctionSg(DaqReaderMultiFunction):
    """
    This class reads data from video files .rec that spikegadgets use

    This implementation does not use the Trodes live API and only works with prerecorded data at the original sample rate.

    Spike Gadgets: http://spikegadgets.com/
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Start of data relative to start of file
        self._data_offset = -1
        # Open and parse file
        self._file_handle = open(self.path, 'rb')
        self._configuration = self._read_configuration()
        self._setup_channels()
        # 1 + sum of numBytes fields TODO - hardcoded here
        self._packet_header_size = 34
        self._channel_size = len(self.probes) * 2
        # Packet header size (MCU/ECU bytes) + timestamp bytes (uint32) + channel bytes (int16)
        self._packet_size = self._packet_header_size + 2 + self._channel_size

    @property
    def _hardware_configuration(self):
        # Shortcut for hardware configuration tag
        return self._configuration.find('HardwareConfiguration')

    @property
    def _spike_configuration(self):
        # Shortcut for spike configuration
        return self._configuration.find('SpikeConfiguration')

    @property
    def _num_channels(self):
        # Get channel count from parsed config
        return int(self._hardware_configuration.attrib['numChannels'])

    @property
    def t0(self):
        return 0

    @property
    def t1(self):
        return self.samples / self.sample_rate

    @property
    def sample_rate(self):
        # Get sample rate from parsed config
        return int(self._hardware_configuration.attrib['samplingRate'])

    @property
    def samples(self):
        return int((self._data_size - self._packet_header_size) / self._packet_size)

    def _read_configuration(self):
        """Read in and parse the configuration XML prefix."""
        configuration_doc = ''
        for line in self._file_handle:
            decoded_line = line.decode('ascii')
            configuration_doc += decoded_line
            # End of XML header
            if '</Configuration>' in decoded_line:
                self._data_offset = self._file_handle.tell()
                break
        # Calculate data bytes here
        file_size = os.fstat(self._file_handle.fileno()).st_size
        self._data_size = file_size - self._data_offset
        return ET.fromstring(configuration_doc)

    def _setup_channels(self):
        """Setup channel readers based on SpikeConfiguration portion of configuration."""
        devices = self._hardware_configuration.findall('Device')
        devices.sort(key=lambda dev: dev.attrib['packetOrderPreference'])
        for SpikeNTrode in self._spike_configuration.findall('SpikeNTrode'):
            ntrode_id = int(SpikeNTrode.attrib['id'])
            ntrode_channels = SpikeNTrode.findall('SpikeChannel')
            # Add ntrode's to channels
            for ref in range(len(ntrode_channels)):
                ntrode = ProbeNTrode(
                    'Tetrode{}'.format(ntrode_id), ref + 1)
                self.probes.append(ntrode)

    def _read_all_samples(self, channels_to_read):
        """
        Given a list of channels, read each packet and read the requested channels.
        
        Returns (np.array, np.ndarray) in shape (timestamps[sample], channel_data[channel][sample])
        """
        channel_count = len(channels_to_read)
        timestamps = np.zeros((self.samples), dtype=np.uint32)
        data = np.ndarray((channel_count, self.samples), dtype=np.int16)
        # Header contains the non-ntrode channels, skip for now
        packet_dtype = np.dtype([('header', np.uint8, self._packet_header_size),
                                 ('timestamp', np.uint32),
                                 ('channel_data', np.int16, len(self.probes))])
        # Reset file position
        self._file_handle.seek(0)
        samples = np.fromfile(
            self._file_handle, dtype=packet_dtype, count=self.samples, offset=self._data_offset)

        for index, sample in enumerate(samples):
            timestamps[index] = sample[1]
            for channel in range(channel_count):
                # Read packet data
                data[channel][index] = sample[2][channels_to_read[channel]]

        data = data.astype(np.float) * 12780 / 65536
        return (timestamps, data)

    def read_channels_epoch_samples(self, channel_type, channels, epoch):
        # TODO - hook up epoch to syncgraph, right now all samples are returned as the device epoch
        channels_to_read = []
        if channel_type == ChannelType.analog_in:
            for ch in channels:
                channels_to_read.append(ch.number * 4 + ch.reference)
        else:
            raise Exception('Only ntrode channels are supported')
        return self._read_all_samples(channels_to_read)
