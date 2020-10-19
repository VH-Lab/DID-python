from .daq_reader import DaqReader
from neo.io import Spike2IO


class CEDSpike2(DaqReader):
    extensions = ('.smr',)

    def __init__(self, filename):
        self.filename = filename
        self.__reader = Spike2IO(filename)

    def samplerate(self, channel_number):
        channel_index = self.__reader.channel_id_to_index([channel_number - 1])
        return self.__reader.get_signal_sampling_rate(channel_index)

    def readchannel(self, channel_number, start_index=0, end_index=None):
        channel_id = channel_number - 1
        raw_signal = self.__reader.get_analogsignal_chunk(channel_ids=[channel_id])
        float_signal = self.__reader.rescale_signal_raw_to_float(raw_signal, channel_ids=[channel_id])
        return float_signal[start_index:end_index, 0]

    def readevents(self, channel_number, start_time=0, end_time=None):
        event_channel_index = [*self.__reader.header['event_channels']['id']].index(str(channel_number - 1))
        event_timestamps = self.__reader.get_event_timestamps(event_channel_index=event_channel_index, t_start=start_time, t_stop=end_time)
        return event_timestamps[0] * self.__reader._time_factor
