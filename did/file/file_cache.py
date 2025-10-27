import os
import time
import shutil
import ast
from .binary_table import BinaryTable
from .file_obj import FileObj

class FileCache:
    CACHE_INFO_FILE_NAME = '.fileCacheInfo.sqlite'

    def __init__(self, directory_name, file_name_characters=32, max_size=100e9, reduce_size=80e9):
        if not os.path.isdir(directory_name):
            os.makedirs(directory_name)

        self.directory_name = directory_name
        self.file_name_characters = file_name_characters

        info_file_name = os.path.join(self.directory_name, self.CACHE_INFO_FILE_NAME)
        self.binary_table = BinaryTable(
            FileObj(info_file_name),
            ['char', 'double', 'uint64'],
            [], [], 0 # record_size, elements_per_column, header_size are not needed
        )

        props = self.binary_table.read_header()
        if props:
            self._load_properties(props)
        else:
            self.max_size = int(max_size)
            self.reduce_size = int(reduce_size)
            self.current_size = 0
            self._save_properties()

    def _load_properties(self, props):
        props = ast.literal_eval(props.decode('utf-8'))
        self.max_size = props['max_size']
        self.reduce_size = props['reduce_size']
        self.current_size = props['current_size']

    def _save_properties(self):
        props = {
            'max_size': self.max_size,
            'reduce_size': self.reduce_size,
            'current_size': self.current_size
        }
        self.binary_table.write_header(str(props).encode('utf-8'))

    def add_file(self, full_path_file_name, file_name_in_cache=None, copy=False):
        if file_name_in_cache is None:
            file_name_in_cache = os.path.basename(full_path_file_name)

        if len(file_name_in_cache) > self.file_name_characters:
            raise ValueError(f"File name has wrong number of characters (expected at most {self.file_name_characters}).")

        row, _ = self.binary_table.find_row(1, file_name_in_cache)
        if row != 0:
            raise ValueError(f"There is already a file with name '{file_name_in_cache}' in the cache.")

        file_size = os.path.getsize(full_path_file_name)
        if self.current_size + file_size > self.max_size:
            self._resize(self.reduce_size - file_size)

        dest_path = os.path.join(self.directory_name, file_name_in_cache)
        if copy:
            shutil.copy2(full_path_file_name, dest_path)
        else:
            shutil.move(full_path_file_name, dest_path)

        self.current_size += file_size
        self.binary_table.insert_row(0, [file_name_in_cache, time.time(), file_size])
        self._save_properties()

    def remove_file(self, file_name_in_cache):
        row, _ = self.binary_table.find_row(1, file_name_in_cache)
        if row == 0:
            raise ValueError(f"File '{file_name_in_cache}' is not in file cache manifest.")

        file_size = self.binary_table.read_row(row, 3)[0]
        self.binary_table.delete_row(row)
        os.remove(os.path.join(self.directory_name, file_name_in_cache))
        self.current_size -= file_size
        self._save_properties()

    def clear(self):
        self.binary_table.write_table([])
        for filename in os.listdir(self.directory_name):
            if filename != self.CACHE_INFO_FILE_NAME:
                os.remove(os.path.join(self.directory_name, filename))
        self.current_size = 0
        self._save_properties()

    def is_file(self, file_name_in_cache):
        row, _ = self.binary_table.find_row(1, file_name_in_cache)
        return row != 0

    def file_list(self):
        names = self.binary_table.read_row(float('inf'), 1)
        access_times = self.binary_table.read_row(float('inf'), 2)
        sizes = self.binary_table.read_row(float('inf'), 3)

        files = []
        for i in range(len(names)):
            files.append({
                'name': names[i],
                'last_access': access_times[i],
                'size': sizes[i]
            })
        return files

    def _resize(self, target_size):
        files = self.file_list()
        if not files:
            return

        files.sort(key=lambda x: x['last_access'])

        total_size = sum(f['size'] for f in files)

        while total_size > target_size and files:
            file_to_remove = files.pop(0)
            self.remove_file(file_to_remove['name'])
            total_size -= file_to_remove['size']

    def touch(self, file_name_in_cache):
        row, _ = self.binary_table.find_row(1, file_name_in_cache)
        if row != 0:
            self.binary_table.write_entry(row, 2, time.time())
