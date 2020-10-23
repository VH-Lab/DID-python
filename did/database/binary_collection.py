from pathlib import Path


class BinaryCollection:
    def __init__(self, dir_path):
        self.dir_path = Path(dir_path)

        # Initializing Collection
        self.dir_path.mkdir(parents=True, exist_ok=True)

    def open_write_stream(self, did_document, name):
        if name not in did_document.data['binary_files']:
            did_document.data['binary_files'].append(name)
        return open(self.dir_path / f'{did_document.id}-{name}.bin', 'wb')

    def open_read_stream(self, did_document, name):
        if name in did_document.data['binary_files']:
            return open(self.dir_path / f'{did_document.id}-{name}.bin', 'rb')
        else:
            raise FileNotFoundError(f'The binary file \'{name}\' was not found in the document.')

    def remove_file(self, did_document, name):
        did_document.data['binary_files'].remove(name)
        (self.dir_path / f'{did_document.id}-{name}.bin').unlink()
