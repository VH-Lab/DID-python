import os
import tempfile
from pathlib import Path
import uuid

def toolboxdir():
    """
    Returns the path to the toolbox directory.
    """
    return os.path.dirname(os.path.abspath(__file__))

def must_be_writable(folder_path):
    """
    Checks if a folder is writable, and creates it if it doesn't exist.
    """
    if not os.path.exists(folder_path):
        try:
            os.makedirs(folder_path)
        except OSError:
            # Fallback for potential permission errors
            folder_path = os.path.join(tempfile.gettempdir(), os.path.basename(folder_path))
            os.makedirs(folder_path, exist_ok=True)

    test_file = os.path.join(folder_path, f"testfile_{uuid.uuid4()}.txt")
    try:
        with open(test_file, 'w') as f:
            f.write('test')
    except IOError:
        raise IOError(f'We do not have write access to the folder at {folder_path}')
    finally:
        if os.path.exists(test_file):
            os.remove(test_file)

class PathConstants:
    """
    Class that defines some global constants for the DID package.
    """
    PATH = toolboxdir()
    DEFPATH = os.path.join(PATH, 'example_schema', 'demo_schema1')

    DEFINITIONS = {
        '$DIDDOCUMENT_EX1': os.path.join(DEFPATH, 'database_documents'),
        '$DIDSCHEMA_EX1': os.path.join(DEFPATH, 'database_schema'),
        '$DIDCONTROLLEDVOCAB_EX1': os.path.join(DEFPATH, 'controlled_vocabulary')
    }

    _temp_path = os.path.join(tempfile.gettempdir(), 'didtemp')
    _file_cache_path = os.path.join(str(Path.home()), 'Documents', 'DID', 'fileCache')
    _preferences_path = os.path.join(str(Path.home()), 'Documents', 'DID', 'Preferences')

    @property
    def temppath(self):
        must_be_writable(self._temp_path)
        return self._temp_path

    @property
    def filecachepath(self):
        must_be_writable(self._file_cache_path)
        return self._file_cache_path

    @property
    def preferences(self):
        must_be_writable(self._preferences_path)
        return self._preferences_path

# Placeholder for fileCache class
class FileCache:
    def __init__(self, path, size):
        self.path = path
        self.size = size

_cached_cache = None

def get_cache():
    """
    Returns a persistent cache object.
    """
    global _cached_cache
    if _cached_cache is None:
        path_constants = PathConstants()
        _cached_cache = FileCache(path_constants.filecachepath, 33)
    return _cached_cache