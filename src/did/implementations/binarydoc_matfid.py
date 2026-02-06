from ..binarydoc import BinaryDoc
from ..file import Fileobj

class BinaryDocMatfid(BinaryDoc, Fileobj):
    def __init__(self, key='', doc_unique_id='', **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self.doc_unique_id = doc_unique_id
        # Ensure machine format is little-endian for cross-platform compatibility
        self.machineformat = 'l'

    def fclose(self):
        super().fclose()
        # Reset properties after closing
        self.permission = 'r'

    # The abstract methods from BinaryDoc would be implemented here,
    # likely by calling the corresponding methods of the Fileobj superclass.

    def fopen(self):
        return super().fopen()

    def fseek(self, location, reference):
        return super().fseek(location, reference)

    def ftell(self):
        return super().ftell()

    def feof(self):
        return super().feof()

    def fwrite(self, data, precision=None, skip=0):
        # The precision and skip parameters would need to be handled
        # using Python's struct module for a full implementation.
        return super().fwrite(data)

    def fread(self, count=-1, precision=None, skip=0):
        # The precision and skip parameters would need to be handled
        # using Python's struct module for a full implementation.
        return super().fread(count)