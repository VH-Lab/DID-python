from pathlib import Path
from did.versioning import hash_document
from contextlib import contextmanager
import re

class BinaryCollection:
    def __init__(self, dir_path, did):
        self.dir_path = Path(dir_path)
        self.did = did

        # Initializing Collection
        self.dir_path.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def open_write_stream(self, did_document, name, save=None):
        filename = self.get_filename(did_document, name)
        if filename not in did_document.data['binary_files']:
            did_document.data['binary_files'].append(filename)
        else:
            self.update_document_filenames(did_document, name)
        with open(self.dir_path / filename, 'wb') as w_stream:
            yield w_stream
        self.did.update(did_document, save=save)

    @contextmanager
    def open_read_stream(self, did_document, name):
        filename = self.get_filename_for_name(did_document, name)
        if filename in did_document.data['binary_files']:
            with open(self.dir_path / filename, 'rb') as r_stream:
                yield r_stream
        else:
            raise FileNotFoundError(f'The binary file \'{name}\' was not found in the document.')

    def remove_file(self, did_document, name):
        did_document.data['binary_files'].remove(self.get_filename_for_name(did_document, name))

    def list_files(self, did_document, snapshot=None):
        filenames = did_document.data['binary_files']
        return [
            self.__extract_name_from_filename(filename)
            for filename in filenames
        ]
    
    def get_filename_for_name(self, did_document, name):
        filenames = [
            filename for filename in did_document.data['binary_files']
            if re.search(f'^{name}--', filename)
        ]
        return filenames[0] if filenames else None

    def get_filename(self, did_document, name, snapshot=None):
        snapshots = did_document.data['base']['snapshots']
        working_snapshot = self.did.driver.working_snapshot_id
        return f'{name}--{did_document.id}-{snapshot or working_snapshot}.bin'
        # '--on_<previous_hash>' ensures that data with the same name cannot exist on the same document in the same snapshot
    
    def get_filepath(self, did_document, name):
        filename = self.get_filename_for_name(did_document, name)
        return self.dir_path / filename

    def update_document_filenames(self, did_document, name):
        working_snapshot = self.did.driver.working_snapshot_id
        filename = get_filename_for(did_document, name)
        did_document.data['binary_files'] = [
            self.__update_filename_to_snapshot(did_document, filename, working_snapshot) if fn == filename else fn
            for fn in did_document.data['binary_files']
        ]
    
    def check_filename_in_snapshot(self, filename, snapshot=None):
        snapshot = str(snapshot or self.did.driver.working_snapshot_id)
        return bool(re.search(f'-{snapshot}\.bin$', filename))

    def __update_filename_to_snapshot(self, did_document, filename, snapshot):
        name = self.__extract_name_from_filename(filename)
        return self.get_filename(did_document, name, snapshot=snapshot)

    def __extract_name_from_filename(self, filename):
        return re.findall('^(.*)--', filename)[0]