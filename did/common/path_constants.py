import os
from appdirs import user_data_dir

class PathConstants:
    """
    Defines standard paths for the DID application.
    """
    APP_NAME = "DID"
    APP_AUTHOR = "VH-Lab"

    # Use appdirs to determine the platform-specific user data directory
    USER_DATA_DIR = user_data_dir(APP_NAME, APP_AUTHOR)

    # Ensure the directory exists
    os.makedirs(USER_DATA_DIR, exist_ok=True)

    FILE_CACHE_PATH = os.path.join(USER_DATA_DIR, "FileCache")
    PREFERENCES = os.path.join(USER_DATA_DIR, "Preferences")

    # Define project root and schema paths
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    SCHEMA_PATH = os.path.join(PROJECT_ROOT, 'schemas')
    DB_DOCS_PATH = os.path.join(SCHEMA_PATH, 'database_documents')
    DB_SCHEMA_PATH = os.path.join(SCHEMA_PATH, 'database_schema')

    DEFINITIONS = {
        '$DID_MATLAB_DIR': PROJECT_ROOT, # Legacy support
        '$DID_SCHEMA_PATH': SCHEMA_PATH,
        '$DIDDOCUMENT_EX1': DB_DOCS_PATH,
        '$DIDSCHEMA_EX1': DB_SCHEMA_PATH
    }
