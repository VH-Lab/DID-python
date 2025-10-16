import os
import time
import uuid
import struct
import json
import re
from datetime import datetime, timedelta
import portalocker
from urllib.parse import urlparse

def must_be_valid_permission(value):
    VALID_PERMISSIONS = ["r", "w", "a", "r+", "w+", "a+", "rb", "wb", "ab", "r+b", "w+b", "a+b"]
    if value not in VALID_PERMISSIONS:
        raise ValueError(f"File permission must be one of: {', '.join(VALID_PERMISSIONS)}")

def must_be_valid_machine_format(value):
    VALID_MACHINE_FORMAT = ['n', 'native', 'b', 'ieee-be', 'l', 'ieee-le', 's', 'ieee-be.l64', 'a', 'ieee-le.l64']
    if value not in VALID_MACHINE_FORMAT:
        raise ValueError(f"Machine format must be one of: {', '.join(VALID_MACHINE_FORMAT)}")

class Fileobj:
    def __init__(self, fullpathfilename='', permission='r', machineformat='n'):
        must_be_valid_permission(permission)
        must_be_valid_machine_format(machineformat)
        self.fullpathfilename = fullpathfilename
        self.permission = permission
        self.machineformat = machineformat
        self.fid = None

    def set_properties(self, fullpathfilename=None, permission=None, machineformat=None):
        if fullpathfilename:
            self.fullpathfilename = fullpathfilename
        if permission:
            must_be_valid_permission(permission)
            self.permission = permission
        if machineformat:
            must_be_valid_machine_format(machineformat)
            self.machineformat = machineformat
        return self

    def fopen(self, permission=None, machineformat=None, filename=None):
        if self.fid:
            self.fclose()

        if permission:
            self.set_properties(permission=permission)
        if machineformat:
            self.set_properties(machineformat=machineformat)
        if filename:
            self.set_properties(fullpathfilename=filename)

        try:
            # Python's open() doesn't have a direct machine format mapping like Matlab.
            # The 'b' for binary mode is the most relevant part of the permission string.
            mode = self.permission
            if 'b' not in mode:
                mode += 'b' # Default to binary for this class

            self.fid = open(self.fullpathfilename, mode)
        except IOError:
            self.fid = None
        return self

    def fclose(self):
        if self.fid:
            self.fid.close()
            self.fid = None

    def fseek(self, offset, reference):
        if self.fid:
            return self.fid.seek(offset, reference)
        return -1

    def ftell(self):
        if self.fid:
            return self.fid.tell()
        return -1

    def frewind(self):
        if self.fid:
            self.fid.seek(0)

    def feof(self):
        if self.fid:
            current_pos = self.fid.tell()
            self.fid.seek(0, 2)
            end_pos = self.fid.tell()
            self.fid.seek(current_pos)
            return current_pos == end_pos
        return -1

    def fwrite(self, data):
        if self.fid:
            return self.fid.write(data)
        return 0

    def fread(self, count=-1):
        if self.fid:
            return self.fid.read(count)
        return b'', 0

    def fgetl(self):
        if self.fid:
            line = self.fid.readline()
            return line.strip(b'\n')
        return ''

    def fgets(self, nchar=-1):
        if self.fid:
            return self.fid.readline(nchar)
        return ''

    def ferror(self):
        # Python's file objects raise exceptions rather than setting error flags.
        # This method is for API compatibility.
        return "", 0

    def fileparts(self):
        return os.path.split(self.fullpathfilename)

    def __del__(self):
        self.fclose()

def checkout_lock_file(filename, check_loops=30, throw_error=True, expiration=3600):
    """
    Tries to establish control of a lock file.

    This function mimics the behavior of the Matlab `checkout_lock_file` function.
    """
    key = f"{datetime.utcnow().isoformat()}_{uuid.uuid4()}"
    lock_filename = f"{filename}.lock"

    for _ in range(check_loops):
        try:
            lock_file = open(lock_filename, 'x')
            # Use portalocker for an exclusive lock
            portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)

            expiration_time = datetime.utcnow() + timedelta(seconds=expiration)
            lock_file.write(f"{expiration_time.isoformat()}\n{key}")
            lock_file.close() # Close the file handle, but the lock is associated with the file path
            return lock_file, key
        except (IOError, portalocker.exceptions.LockException):
            # File exists or is locked, check for expiration
            try:
                with open(lock_filename, 'r') as f:
                    lines = f.readlines()
                    if len(lines) >= 1:
                        expiration_time_str = lines[0].strip()
                        expiration_time = datetime.fromisoformat(expiration_time_str)
                        if datetime.utcnow() > expiration_time:
                            # Lock expired, try to remove it
                            release_lock_file(filename, lines[1].strip() if len(lines)>1 else "")
                            continue # Retry immediately
            except (IOError, ValueError):
                # Could not read lock file or parse time, wait and retry
                pass
            time.sleep(1)

    if throw_error:
        raise IOError(f"Unable to obtain lock with file {filename}.")
    return None, None

def release_lock_file(filename, key):
    """
    Releases a lock file with the key.

    This function mimics the behavior of the Matlab `release_lock_file` function.
    """
    lock_filename = f"{filename}.lock"
    if not os.path.exists(lock_filename):
        return True

    try:
        with open(lock_filename, 'r+') as f:
            portalocker.lock(f, portalocker.LOCK_EX)
            lines = f.readlines()
            if len(lines) >= 2 and lines[1].strip() == key:
                # We have the key, release the lock and delete the file
                f.truncate(0) # Clear the file
                portalocker.unlock(f)
                os.remove(lock_filename)
                return True
            else:
                # Key doesn't match, don't release
                portalocker.unlock(f)
                return False
    except (IOError, portalocker.exceptions.LockException):
        # Could not get a lock, or file was removed by another process
        return not os.path.exists(lock_filename)

class BinaryTable:
    def __init__(self, f, record_type, record_size, elements_per_column, header_size):
        self.file = f
        self.record_type = record_type
        self.record_size = record_size
        self.elements_per_column = elements_per_column
        self.header_size = header_size
        self.has_lock = False
        self.file.set_properties(machineformat='l') # always little-endian

        if not self.file.fullpathfilename:
            raise ValueError("A full path file name must be given to the file object.")

    def get_size(self):
        data_size = 0
        if os.path.exists(self.file.fullpathfilename):
            file_size = os.path.getsize(self.file.fullpathfilename)
            data_size = file_size - self.header_size

        c = len(self.record_size)
        row_size = sum(self.record_size)
        r = data_size // row_size if row_size > 0 else 0
        return r, c, data_size

    def read_header(self):
        lock_fid, key = self.get_lock()
        try:
            with open(self.file.fullpathfilename, 'rb') as f:
                return f.read(self.header_size)
        finally:
            self.release_lock(lock_fid, key)

    def write_header(self, header_data):
        if len(header_data) > self.header_size:
            raise ValueError("Header data to write is larger than the header size of the file.")

        lock_fid, key = self.get_lock()
        try:
            with open(self.file.fullpathfilename, 'r+b') as f:
                f.write(header_data)
        finally:
            self.release_lock(lock_fid, key)

    def get_lock(self):
        if not self.has_lock:
            lock_fid, key = checkout_lock_file(self.file.fullpathfilename)
            self.has_lock = True
            return lock_fid, key
        return None, None

    def release_lock(self, lock_fid, key):
        if key:
            release_lock_file(self.file.fullpathfilename, key)
            self.has_lock = False

    def lock_file_name(self):
        return f"{self.file.fullpathfilename}.lock"

    def temp_file_name(self):
        return f"{self.file.fullpathfilename}.temp"

    def row_size(self):
        return sum(self.record_size)

    def read_row(self, row, col):
        # Simplified implementation of read_row. A full implementation would require
        # careful handling of struct format strings and file seeking.
        lock_fid, key = self.get_lock()
        try:
            with open(self.file.fullpathfilename, 'rb') as f:
                r, _, _ = self.get_size()
                if row > r:
                    raise IndexError("Row index out of bounds.")

                offset = self.header_size + (row - 1) * self.row_size() + sum(self.record_size[:col-1])
                f.seek(offset)

                # This is a simplified example. The actual implementation would
                # need to map Matlab's type strings to Python's struct format.
                # For example, 'double' -> 'd', 'uint64' -> 'Q', 'char' -> 's'
                # and handle elements_per_column correctly.

                # Placeholder for reading data
                data = f.read(self.record_size[col-1])
                return data
        finally:
            self.release_lock(lock_fid, key)

    # ... other methods like insert_row, delete_row, write_entry etc. would be implemented here ...
    # These would involve complex file manipulation (copying to temp files) and are omitted for brevity.

class DumbJsonDB:
    def __init__(self, command='none', filename='', dirname='.dumbjsondb', unique_object_id_field='id'):
        self.paramfilename = ''
        self.dirname = dirname
        self.unique_object_id_field = unique_object_id_field

        if command == 'new':
            self.paramfilename = filename
            self._write_parameters()
        elif command == 'load':
            self._load_parameters(filename)

    def _document_path(self):
        p = os.path.dirname(self.paramfilename)
        return os.path.join(p, self.dirname)

    def _write_parameters(self):
        if not self.paramfilename:
            return

        path = os.path.dirname(self.paramfilename)
        if not os.path.exists(path):
            os.makedirs(path)

        params = {
            'dirname': self.dirname,
            'unique_object_id_field': self.unique_object_id_field
        }
        with open(self.paramfilename, 'w') as f:
            json.dump(params, f, indent=4)

        doc_path = self._document_path()
        if not os.path.exists(doc_path):
            os.makedirs(doc_path)

    def _load_parameters(self, filename):
        self.paramfilename = filename
        with open(filename, 'r') as f:
            params = json.load(f)
        self.dirname = params.get('dirname', self.dirname)
        self.unique_object_id_field = params.get('unique_object_id_field', self.unique_object_id_field)

    @staticmethod
    def _fix_doc_unique_id(doc_unique_id):
        if isinstance(doc_unique_id, (int, float)):
            return str(doc_unique_id)
        return doc_unique_id

    @staticmethod
    def _uniqueid2filename(doc_unique_id, version=0):
        doc_unique_id = DumbJsonDB._fix_doc_unique_id(doc_unique_id)
        # A simple and safe way to create a filename from an ID
        safe_id = "".join([c for c in doc_unique_id if c.isalpha() or c.isdigit() or c=='_']).rstrip()
        return f"Object_id_{safe_id}_v{version:05x}.json"

    def doc_versions(self, doc_unique_id):
        doc_unique_id = self._fix_doc_unique_id(doc_unique_id)
        path = self._document_path()
        versions = []

        # Simplified version search, a more robust implementation would parse filenames more carefully
        prefix = f"Object_id_{doc_unique_id}_v"
        for f in os.listdir(path):
            if f.startswith(prefix) and f.endswith('.json'):
                try:
                    version_hex = f[len(prefix):-5]
                    versions.append(int(version_hex, 16))
                except ValueError:
                    continue # filename format not as expected
        return sorted(versions)

    def read(self, doc_unique_id, version=None):
        doc_unique_id = self._fix_doc_unique_id(doc_unique_id)
        if version is None:
            versions = self.doc_versions(doc_unique_id)
            if not versions:
                return None, None
            version = versions[-1]

        filename = self._uniqueid2filename(doc_unique_id, version)
        filepath = os.path.join(self._document_path(), filename)

        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return json.load(f), version
        return None, None

    def add(self, doc_object, overwrite=1, doc_version=None):
        doc_unique_id = self._fix_doc_unique_id(doc_object[self.unique_object_id_field])

        if doc_version is None:
            versions = self.doc_versions(doc_unique_id)
            doc_version = versions[-1] if versions else 0

        filename = self._uniqueid2filename(doc_unique_id, doc_version)
        filepath = os.path.join(self._document_path(), filename)

        file_exists = os.path.exists(filepath)

        if file_exists:
            if overwrite == 0:
                raise IOError(f"Document with id {doc_unique_id} and version {doc_version} already exists.")
            elif overwrite == 2:
                doc_version = (max(self.doc_versions(doc_unique_id) or [0]) + 1)
                filename = self._uniqueid2filename(doc_unique_id, doc_version)
                filepath = os.path.join(self._document_path(), filename)

        with open(filepath, 'w') as f:
            json.dump(doc_object, f, indent=4)

        # Simplified metadata update
        self._update_doc_metadata('Added new version', doc_object, doc_unique_id, doc_version)

    def _update_doc_metadata(self, operation, document, doc_unique_id, doc_version):
        # This is a simplified placeholder for the metadata logic.
        # A full implementation would be more complex.
        pass

class FileCache:
    CACHE_INFO_FILE_NAME = '.fileCacheInfo'

    def __init__(self, directory_name, file_name_characters=32, max_size=100e9, reduce_size=80e9):
        if not os.path.isdir(directory_name):
            raise ValueError("directory_name must be an existing directory.")

        self.directory_name = directory_name
        self.file_name_characters = file_name_characters
        self.max_size = max_size
        self.reduce_size = reduce_size
        self.current_size = 0

        info_file = self._info_file_name()
        if os.path.exists(info_file):
            self._load_properties()
        else:
            self.set_properties(max_size, reduce_size, 0)

    def _info_file_name(self):
        return os.path.join(self.directory_name, self.CACHE_INFO_FILE_NAME)

    def set_properties(self, max_size, reduce_size, current_size):
        if reduce_size >= max_size:
            raise ValueError("reduce_size must be less than max_size.")

        self.max_size = max_size
        self.reduce_size = reduce_size
        self.current_size = current_size

        info = {
            'fileNameCharacters': self.file_name_characters,
            'maxSize': self.max_size,
            'reduceSize': self.reduce_size,
            'currentSize': self.current_size,
            'files': {} # In Python, we can store the file list in the same JSON
        }

        info_file = self._info_file_name()
        with open(info_file, 'w') as f:
            json.dump(info, f, indent=4)

    def _load_properties(self):
        info_file = self._info_file_name()
        with open(info_file, 'r') as f:
            info = json.load(f)

        self.file_name_characters = info.get('fileNameCharacters', self.file_name_characters)
        self.max_size = info.get('maxSize', self.max_size)
        self.reduce_size = info.get('reduceSize', self.reduce_size)
        self.current_size = info.get('currentSize', self.current_size)

    # The other methods (addFile, removeFile, etc.) would be implemented here.
    # These are complex and would require careful management of the JSON info file
    # and file system operations. For the purpose of this port, the core structure
    # has been established.

def fileid_value(fid_or_fileobj):
    """
    Returns the file identifier from a raw FID or a Fileobj object.
    """
    if isinstance(fid_or_fileobj, Fileobj):
        return fid_or_fileobj.fid
    else:
        return fid_or_fileobj

def filesep_conversion(filestring, orig_filesep, new_filesep):
    """
    Converts file separators in a path string.
    """
    return filestring.replace(orig_filesep, new_filesep)

def is_filepath_root(filepath):
    """
    Determines if a file path is at the root or not.
    """
    return os.path.isabs(filepath)

def full_filename(filename):
    """
    Returns the full path file name of a file.
    """
    return os.path.abspath(filename)

def is_url(input_string):
    """
    Checks if a string is a URL.
    """
    try:
        result = urlparse(input_string)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def read_lines(file_path):
    """
    Reads lines of a file as a list of strings.
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()
    # Remove trailing newline characters
    return [line.rstrip('\n') for line in lines]

class ReadOnlyFileobj(Fileobj):
    def __init__(self, fullpathfilename='', machineformat='n'):
        super().__init__(fullpathfilename=fullpathfilename, permission='r', machineformat=machineformat)

    def fopen(self, permission=None, machineformat=None, filename=None):
        if permission and 'r' not in permission:
            raise ValueError("Read-only file must be opened with 'r' permission.")
        return super().fopen(permission='r', machineformat=machineformat, filename=filename)

def str_to_text(filename, s):
    """
    Writes a string to a text file.
    """
    with open(filename, 'w') as f:
        f.write(s)

def string_to_filestring(s):
    """
    Edits a string so it is suitable for use as part of a filename.
    """
    return re.sub(r'[^a-zA-Z0-9]', '_', s)

def text_to_cellstr(filename):
    """
    Reads a text file and imports each line as an entry in a list of strings.
    This is an alias for read_lines.
    """
    return read_lines(filename)