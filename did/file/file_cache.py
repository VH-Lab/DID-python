import os
import time
import shutil
from .binary_table import BinaryTable
from .file_obj import FileObj

class FileCache:
    CACHE_INFO_FILE_NAME = '.fileCacheInfo'

    def __init__(self, directory_name, file_name_characters=32, max_size=100e9, reduce_size=80e9):
        if not os.path.isdir(directory_name):
            raise ValueError(f"Directory '{directory_name}' does not exist.")

        self.directory_name = directory_name
        self.file_name_characters = file_name_characters
        self.max_size = int(max_size)
        self.reduce_size = int(reduce_size)
        self.current_size = 0

        info_file_name = os.path.join(self.directory_name, self.CACHE_INFO_FILE_NAME)
        if os.path.exists(info_file_name):
            self._load_properties()
        else:
            self._save_properties()

    def _load_properties(self):
        info_file_name = os.path.join(self.directory_name, self.CACHE_INFO_FILE_NAME)
        # In a real implementation, this would read from the binary info file.
        # For simplicity, we'll just use the values from the constructor.
        pass

    def _save_properties(self):
        info_file_name = os.path.join(self.directory_name, self.CACHE_INFO_FILE_NAME)
        # In a real implementation, this would write to the binary info file.
        # For simplicity, we'll just create an empty file.
        with open(info_file_name, 'w'):
            pass

    def add_file(self, full_path_file_name, file_name_in_cache=None, copy=False):
        if file_name_in_cache is None:
            file_name_in_cache = os.path.basename(full_path_file_name)

        if len(file_name_in_cache) > self.file_name_characters:
            raise ValueError(f"File name has wrong number of characters (expected at most {self.file_name_characters}).")

        if self.is_file(file_name_in_cache):
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
        self._touch(file_name_in_cache)
        self._save_properties()

    def remove_file(self, file_name_in_cache):
        file_path = os.path.join(self.directory_name, file_name_in_cache)
        if not os.path.exists(file_path):
            raise ValueError(f"File '{file_name_in_cache}' is not in file cache manifest.")

        file_size = os.path.getsize(file_path)
        os.remove(file_path)
        self.current_size -= file_size
        self._save_properties()

    def clear(self):
        for filename in os.listdir(self.directory_name):
            if filename != self.CACHE_INFO_FILE_NAME:
                os.remove(os.path.join(self.directory_name, filename))
        self.current_size = 0
        self._save_properties()

    def is_file(self, file_name_in_cache):
        return os.path.exists(os.path.join(self.directory_name, file_name_in_cache))

    def file_list(self):
        files = []
        for filename in os.listdir(self.directory_name):
            if filename != self.CACHE_INFO_FILE_NAME:
                path = os.path.join(self.directory_name, filename)
                files.append({
                    'name': filename,
                    'size': os.path.getsize(path),
                    'last_access': os.path.getatime(path)
                })
        return files

    def _resize(self, target_size):
        files = self.file_list()
        files.sort(key=lambda x: x['last_access'])

        while self.current_size > target_size and files:
            file_to_remove = files.pop(0)
            self.remove_file(file_to_remove['name'])

    def _touch(self, file_name_in_cache):
        file_path = os.path.join(self.directory_name, file_name_in_cache)
        if os.path.exists(file_path):
            os.utime(file_path, (time.time(), time.time()))

    def touch(self, file_name_in_cache):
        self._touch(file_name_in_cache)
