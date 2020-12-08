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

    def open_read_stream(self, did_document, name, record=None):
        if name in did_document.data['binary_files']:
            return open(self.get_filepath(did_document, name, record=record), 'rb')
        else:
            raise FileNotFoundError(f'The binary file \'{name}\' was not found in the document.')

    def remove_file(self, did_document, name):
        did_document.data['binary_files'].remove(name)
        self.get_filepath(did_document, name).unlink()

    def list_files(self, did_document):
        return did_document.data['binary_files']
    
    def get_filepath(self, did_document, name, record=None):
        records = did_document.data['base']['records']
        latest_record = records[0] if records else 'NEW'
        return self.dir_path / f'{did_document.id}-{name}--on_{record or latest_record}.bin'
        # '--on_<previous_hash>' ensures that data with the same name cannot exist on the same document in the same snapshot


