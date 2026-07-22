from ..binarydoc import BinaryDoc
from ..file import Fileobj


class BinaryDocMatfid(Fileobj, BinaryDoc):
    # NOTE: base order is (Fileobj, BinaryDoc). Previously it was
    # (BinaryDoc, Fileobj): BinaryDoc.__init__ is a bare ``pass`` that never
    # calls super().__init__(), so Fileobj.__init__ never ran (fullpathfilename
    # / permission / fid were unset) and every ``super().f*`` call resolved to
    # BinaryDoc's abstract no-op stub, so fopen() silently returned None without
    # opening anything. With Fileobj first in the MRO, __init__ initializes the
    # file object and the f* methods delegate to real implementations.
    def __init__(self, key="", doc_unique_id="", **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self.doc_unique_id = doc_unique_id
        # Ensure machine format is little-endian for cross-platform compatibility
        self.machineformat = "l"

    def fclose(self):
        super().fclose()
        # Reset properties after closing
        self.permission = "r"

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
        # precision/skip (MATLAB struct-format writes) are not implemented yet.
        # Fail loudly rather than silently ignoring them and writing raw bytes.
        if precision is not None or skip:
            raise NotImplementedError(
                "BinaryDocMatfid.fwrite does not yet support precision/skip; "
                "only raw byte writes are supported."
            )
        return super().fwrite(data)

    def fread(self, count=-1, precision=None, skip=0):
        # precision/skip (MATLAB struct-format reads) are not implemented yet.
        # Raise rather than returning None so a caller can't mistake an
        # unimplemented typed read for an empty file.
        if precision is not None or skip:
            raise NotImplementedError(
                "BinaryDocMatfid.fread does not yet support precision/skip; "
                "only raw byte reads are supported."
            )
        return super().fread(count)
