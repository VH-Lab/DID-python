"""Tests for BinaryDocMatfid Fileobj initialization and fail-loud behavior.

BinaryDocMatfid used to declare its bases as (BinaryDoc, Fileobj). BinaryDoc's
__init__ is a bare ``pass`` that never calls super().__init__(), so
Fileobj.__init__ never ran and every ``super().f*`` call resolved to BinaryDoc's
abstract no-op stub — fopen() returned None without opening anything. Bases are
now (Fileobj, BinaryDoc), so the Fileobj half is initialized and the file
operations delegate to real implementations.
"""

import os
import tempfile
import unittest

from did.implementations.binarydoc_matfid import BinaryDocMatfid


class TestBinaryDocMatfid(unittest.TestCase):
    def test_init_sets_fileobj_attributes(self):
        b = BinaryDocMatfid()
        # Fileobj.__init__ must have run.
        self.assertTrue(hasattr(b, "fullpathfilename"))
        self.assertTrue(hasattr(b, "permission"))
        self.assertEqual(b.machineformat, "l")
        self.assertIsNone(b.fid)

    def test_fopen_actually_opens_a_file(self):
        fd, path = tempfile.mkstemp()
        os.close(fd)
        try:
            with open(path, "wb") as f:
                f.write(b"hello")
            b = BinaryDocMatfid(fullpathfilename=path, permission="r")
            b.fopen()
            self.assertIsNotNone(b.fid)  # a real file handle, not None
            self.assertEqual(b.fread(5), b"hello")
            b.fclose()
        finally:
            os.remove(path)

    def test_fread_with_precision_raises_notimplemented(self):
        b = BinaryDocMatfid()
        with self.assertRaises(NotImplementedError):
            b.fread(10, precision="double")

    def test_fwrite_with_skip_raises_notimplemented(self):
        b = BinaryDocMatfid()
        with self.assertRaises(NotImplementedError):
            b.fwrite(b"x", skip=4)


if __name__ == "__main__":
    unittest.main()
