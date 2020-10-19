from ndi.database.file_system import BinaryCollection
import numpy as np
import struct
import pytest
import io
from tests.utils import rmrf


def randarray(): return np.random.random(200000)


@pytest.fixture
def binary_collection():
    # Create collection
    collection = BinaryCollection('./test_db', 'document')
    test_array = randarray()

    # Add a file to the collection to use for testing
    (collection.collection_dir / 'test_id.bin').write_bytes(b'hello test' + test_array.tobytes())
    yield collection, collection.collection_dir, test_array
    rmrf(collection.collection_dir)


class TestBinaryCollection:
    def test_directory_creation(self, binary_collection):
        # Test the collection directory was made
        _, collection_dir, _ = binary_collection
        assert collection_dir.exists()
        assert collection_dir.is_dir()

    def test_write_stream(self, binary_collection):
        collection, collection_dir, _ = binary_collection
        test_arr = randarray()

        with collection.open_write_stream('fake_id') as write_stream:
            # Test that the write stream is an io object
            assert isinstance(write_stream, io.BufferedIOBase)

            # Writing contents into file
            write_stream.write(b'hello test')
            for item in test_arr:
                write_stream.write(struct.pack('d', item))

        # Test that binary file was made
        assert (collection_dir / 'fake_id.bin').exists()
        assert (collection_dir / 'fake_id.bin').is_file()

        # Pulling file contents into variables
        file_buffer = (collection_dir / 'fake_id.bin').read_bytes()
        hello_test = file_buffer[0:10]
        array_content = np.frombuffer(file_buffer[10:], dtype=float)

        # Test that contents are what was written
        assert hello_test == b'hello test'
        for i, item in enumerate(array_content):
            assert item == test_arr[i]

    def test_read_stream(self, binary_collection):
        collection, _, test_arr = binary_collection

        with collection.open_read_stream('test_id') as read_stream:
            # Test that the write stream is an io object
            assert isinstance(read_stream, io.BufferedIOBase)

            # Test that first 10 bytes are as expected
            assert read_stream.read(10) == b'hello test'

            # Test that stream offset is at 10 after reading 10 bytes
            assert read_stream.tell() == 10

            # Move stream offset to 18
            read_stream.seek(18)

            # Test that stream offset is at 18 after calling seek()
            assert read_stream.tell() == 18

            # Test that the values from the rest of the stream are as expected
            assert read_stream.read(8) == struct.pack('d', test_arr[1])
            for i, item in enumerate(test_arr[2:]):
                assert read_stream.read(8) == struct.pack('d', item)
