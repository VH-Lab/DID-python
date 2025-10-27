import os
import tempfile
from pathlib import Path

def _ensure_writable(folder_path):
    """
    Ensures that a folder exists and is writable.
    """
    p = Path(folder_path)
    p.mkdir(parents=True, exist_ok=True)

    test_file = p / f"testfile_{os.urandom(8).hex()}.txt"
    try:
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
    except OSError as e:
        raise OSError(f"We do not have write access to the folder at {folder_path}") from e

class PathConstants:
    """
    Defines some global constants for the DID package.
    """

    PATH = Path(__file__).resolve().parent.parent.parent
    DEF_PATH = PATH / 'schemas'

    DEFINITIONS = {
        '$DIDDOCUMENT_EX1': str(DEF_PATH / 'database_documents'),
        '$DIDSCHEMA_EX1': str(DEF_PATH / 'database_schema'),
    }

    TEMP_PATH = os.path.join(tempfile.gettempdir(), 'didtemp')
    _ensure_writable(TEMP_PATH)

    FILE_CACHE_PATH = os.path.join(Path.home(), 'Documents', 'DID', 'fileCache')
    _ensure_writable(FILE_CACHE_PATH)

    PREFERENCES = os.path.join(Path.home(), 'Documents', 'DID', 'Preferences')
    _ensure_writable(PREFERENCES)
