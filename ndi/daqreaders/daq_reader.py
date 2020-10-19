from abc import ABC, abstractmethod


class DaqReader(ABC):
    def __init__(self, filename):
        self.filename = filename

    @abstractmethod
    def samplerate(self, channel_number):
        pass

    @abstractmethod
    def readchannel(self, channel_number, start_time, end_time):
        pass

    @abstractmethod
    def readevents(self, channel_number, start_time=0, end_time=None):
        pass
