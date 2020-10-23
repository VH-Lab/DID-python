from did.database.binary_collection import BinaryCollection
from did.document import DIDDocument
from tests.utils import rmrf
import numpy as np
import pytest
from pathlib import Path
import struct
import io


def randarray(): return np.random.random(200000)

@pytest.fixture
def test_fixture():
    did_doc = DIDDocument(data={
        'base': {
            'id': 1234
        },
        'binary_files': ['from_fixture']
        })
    # Create collection
    collection = BinaryCollection('./test_dir')
    test_array = randarray()

    # Add a file to the collection to use for testing
    (collection.dir_path /
     f'{did_doc.id}-from_fixture.bin').write_bytes(b'hello test' + test_array.tobytes())

    yield collection, did_doc, test_array

    # Remove collection after tests
    rmrf(collection.dir_path)


class TestBinaryCollection:
    def test_directory_creation(self, test_fixture):
        # Test the collection directory was made
        collection, _, _ = test_fixture
        assert collection.dir_path.exists()
        assert collection.dir_path.is_dir()

    def test_open_write_stream(self, test_fixture):
        collection, did_doc, test_array = test_fixture

        with collection.open_write_stream(did_doc, 'new_file') as ws:
            # Test that the write stream is an io object
            assert isinstance(ws, io.BufferedIOBase)

            # Writing contents into file
            ws.write(b'hello test')
            for item in test_array:
                ws.write(struct.pack('d', item))

        # Test that the new binary file name has been added to the document
        assert did_doc.data['binary_files'] == ['from_fixture', 'new_file']

        new_file = Path('./test_dir') / f'{did_doc.id}-new_file.bin'

        # Test that binary file was made
        assert new_file.exists()
        assert new_file.is_file()

        # Pulling file contents into variables
        file_buffer = new_file.read_bytes()
        hello_test = file_buffer[0:10]
        array_content = np.frombuffer(file_buffer[10:], dtype=float)

        # Test that contents are what was written
        assert hello_test == b'hello test'
        for i, item in enumerate(array_content):
            assert item == test_array[i]

    def test_open_read_stream(self, test_fixture):
        collection, did_doc, test_array = test_fixture

        with collection.open_read_stream(did_doc, 'from_fixture') as rs:
            # Test that the write stream is an io object
            assert isinstance(rs, io.BufferedIOBase)

            # Test that first 10 bytes are as expected
            assert rs.read(10) == b'hello test'

            # Test that stream offset is at 10 after reading 10 bytes
            assert rs.tell() == 10

            # Move stream offset to 18
            rs.seek(18)

            # Test that stream offset is at 18 after calling seek()
            assert rs.tell() == 18

            # Test that the values from the rest of the stream are as expected
            assert rs.read(8) == struct.pack('d', test_array[1])
            for item in test_array[2:]:
                assert rs.read(8) == struct.pack('d', item)

    def test_remove_file(self, test_fixture):
        collection, did_doc, _ = test_fixture

        collection.remove_file(did_doc, 'from_fixture')

        # Test that the new binary file name has been remove from the document
        assert did_doc.data['binary_files'] == []
        assert not (collection.dir_path /
                    f'{did_doc.id}-from_fixture.bin').exists()
