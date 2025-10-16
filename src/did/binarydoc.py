import abc

class BinaryDoc(abc.ABC):
    def __init__(self, *args, **kwargs):
        pass

    def __del__(self):
        try:
            self.fclose()
        except:
            pass

    @abc.abstractmethod
    def fopen(self):
        pass

    @abc.abstractmethod
    def fseek(self, location, reference):
        pass

    @abc.abstractmethod
    def ftell(self):
        pass

    @abc.abstractmethod
    def feof(self):
        pass

    @abc.abstractmethod
    def fwrite(self, data, precision, skip):
        pass

    @abc.abstractmethod
    def fread(self, count, precision, skip):
        pass

    @abc.abstractmethod
    def fclose(self):
        pass