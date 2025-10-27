from abc import ABC, abstractmethod

class BinaryDoc(ABC):
    """
    A class for handling binary files and streams.

    This class defines the operations of a binary file/stream and is an abstract
    base class. Subclasses must implement the abstract methods defined here.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the BinaryDoc object.

        As this is an abstract class, this method does nothing.
        """
        pass

    def __del__(self):
        """
        Closes the binary document and deletes its handle.
        """
        self.fclose()

    @abstractmethod
    def fopen(self):
        """
        Opens the binary document for reading/writing.
        """
        pass

    @abstractmethod
    def fseek(self, location, reference):
        """
        Moves to a location within the file stream.

        Args:
            location (int): The location in bytes to move to.
            reference (str): The reference point for the location.
                'bof' - beginning of file
                'cof' - current position in file
                'eof' - end of file
        """
        pass

    @abstractmethod
    def ftell(self):
        """
        Returns the current location in a binary document.

        Returns:
            int: The current location in bytes in the file stream.
        """
        pass

    @abstractmethod
    def feof(self):
        """
        Checks if the binary document is at the end of the file.

        Returns:
            bool: True if the end-of-file indicator is set, False otherwise.
        """
        pass

    @abstractmethod
    def fwrite(self, data, precision, skip):
        """
        Writes data to a binary document.

        Args:
            data: The data to write.
            precision (str): The precision of the data.
            skip (int): The number of bytes to skip after each write.

        Returns:
            int: The number of elements written.
        """
        pass

    @abstractmethod
    def fread(self, count, precision, skip):
        """
        Reads data from a binary document.

        Args:
            count (int): The number of data objects to read.
            precision (str): The precision of the data.
            skip (int): The number of bytes to skip after each read.

        Returns:
            tuple: A tuple containing the data read and the actual count of elements.
        """
        pass

    @abstractmethod
    def fclose(self):
        """
        Closes the binary document.
        """
        pass
