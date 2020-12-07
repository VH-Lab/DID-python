from pathlib import Path
from did.versioning import hash_document


class BinaryCollection:
    def __init__(self, dir_path):
        self.dir_path = Path(dir_path)

        # Initializing Collection
        self.dir_path.mkdir(parents=True, exist_ok=True)

    def open_write_stream(self, did_document, name):
        if name not in did_document.data['binary_files']:
            did_document.data['binary_files'].append(name)
        return open(self.get_filepath(did_document, name), 'wb')

    def open_read_stream(self, did_document, name):
        if name in did_document.data['binary_files']:
            return open(self.get_filepath(did_document, name), 'rb')
        else:
            raise FileNotFoundError(f'The binary file \'{name}\' was not found in the document.')

    def remove_file(self, did_document, name):
        did_document.data['binary_files'].remove(name)
        (self.dir_path / f'{did_document.id}-{name}.bin').unlink()

    def list_files(self, did_document):
        return did_document.data['binary_files']
    
    def get_filepath(self, did_document, name):
        hash_ = hash_document(did_document)
        return self.dir_path / f'{did_document.id}-{name}--{hash_}.bin'

