import os
import dotenv
import did
import json
from urllib.parse import quote_plus


ENV = os.path.join(os.getcwd(), 'config.env')
SUPPORTED_DB = {'mongodb', 'postgres'}
SUPPORTED_BINARY = {'file_system', 'gridfs'}

# Look for config.env in the current working directory.
# If config.env cannot be found, look for config.env in where the module is loaded.
if not os.path.isfile(ENV):
    INSTALLATION = os.path.split(os.path.abspath(did.__path__[0]))[0]
    ENV = os.path.join(INSTALLATION, 'config.env')
    if not os.path.isfile(ENV):
        env = open(ENV, 'w')
        env.close()
else:
    INSTALLATION = os.getcwd()


def _initialize():
    __config__ = dotenv.dotenv_values(ENV)

    # check if document_path and schema_path has been override
    if 'DIDDOCUMENTPATH' not in __config__:
        dotenv.set_key(ENV, 'DIDDOCUMENTPATH', os.path.join(
            INSTALLATION, 'schema', 'database-documents'))
    if 'DIDSCHEMAPATH' not in __config__:
        dotenv.set_key(ENV, 'DIDSCHEMAPATH', os.path.join(
            INSTALLATION, 'schema', 'validation-documents'))
    if 'FILESEP' not in __config__:
        dotenv.set_key(ENV, 'FILESEP', '/')
    if 'MONGO_CONNECTION_STRING' not in __config__:
        dotenv.set_key(ENV, 'MONGO_CONNECTION_STRING',
                       "mongodb://localhost:27017")
    if 'POSTGRES_CONNECTION_STRING' not in __config__:
        dotenv.set_key(ENV, 'POSTGRES_CONNECTION_STRING',
                       "postgres://postgres:password@localhost:5432/did_versioning_tests")


_initialize()


def load_external_configuration(filepath):
    global ENV
    global INSTALLATION
    ENV = filepath
    INSTALLATION = os.path.split(ENV)[0]
    _initialize()


def get_db_configuration():
    """
    Get the database initiation configuration
    """
    config = json.loads(get_variable('DB'))
    if not isinstance(config, dict) or 'db' not in config or 'binary' not in config:
        raise TypeError(
            "the DB variable needs to be key-value pairs including both db and binary as keys")
    db = config['db']
    binary = config['binary']
    if not isinstance(db, dict) or 'type' not in db or 'args' not in db:
        raise TypeError("db type and its args needs to be specified")
    if not isinstance(binary, dict) or 'type' not in binary or 'args' not in binary:
        raise TypeError("filesystem type and its args needs to be specified")
    if db['type'] not in SUPPORTED_DB:
        raise TypeError("db {} not supported. Please choose from the following list {}".format(
            db['type'], SUPPORTED_DB)
        )
    if binary['type'] not in SUPPORTED_BINARY:
        raise TypeError("binary_system {} not supported. Please choose from the following list {}".format(
            binary['type'], SUPPORTED_BINARY)
        )
    return (db, binary)

def which_config():
    return ENV

def set_db_configuration(db_type, db_option, filesystem_type, binary_option):
    if db_type not in SUPPORTED_DB:
        raise ValueError("db{} is not supported. Please choose from the following db software {}".format(
            db_type, SUPPORTED_DB))
    if filesystem_type not in SUPPORTED_BINARY:
        raise ValueError("filesystem {} is not supported. Please choose from the following file system {}".format(
            filesystem_type, SUPPORTED_BINARY))
    if not isinstance(db_option, dict) or not isinstance(binary_option, dict):
        raise ValueError(
            "db_option and binary_option needs to be a key-value pairs")
    db_config = {
        'db': {
            'type': db_type,
            'args': db_option
        },
        'binary': {
            'type': filesystem_type,
            'args': binary_option
        }
    }
    dotenv.set_key(ENV, 'DB', json.dumps(db_config))
    return db_config


def create_did():
    """
    Create an instance of DID given the setting specified in 
    """
    from .core import DID
    from .database import Mongo, SQL, GridFSBinary, FileSystem

    SUPPORTED_DB = {'mongodb': Mongo, 'postgres': SQL}
    SUPPORTED_BINARY = {'file_system': FileSystem, 'gridfs': GridFSBinary}

    db, binary = get_db_configuration()
    db_args = db['args']
    binary_args = binary['args']
    db_instance = SUPPORTED_DB[db['type']](**db_args)
    binary_instance = SUPPORTED_BINARY[binary['type']](**binary_args)
    return DID(driver=db_instance, binary_driver=binary_instance)


def revert_to_default():
    """
    Revert all global variables 'DIDDOCUMENTPATH', 'DIDSCHEMAPATH', 'FILESEP', 'MONGO_CONNECTION_STRING'
    back to its default state
    """
    dotenv.set_key(ENV, 'DIDDOCUMENTPATH', os.path.join(
        INSTALLATION, 'schema', 'database-documents'))
    dotenv.set_key(ENV, 'DIDSCHEMAPATH', os.path.join(
        INSTALLATION, 'schema', 'validation-documents'))
    dotenv.set_key(ENV, 'FILESEP', '/')
    dotenv.set_key(ENV, 'MONGO_CONNECTION_STRING', "mongodb://localhost:27017")
    dotenv.set_key(ENV, 'POSTGRES_CONNECTION_STRING',
                   "postgres://postgres:password@localhost:5432/did_versioning_tests")


def set_documentpath(path):
    """
    Set the path to the DID-document schema, from where the document class will be looking for the
    corresponding .json file when constructing itself. DOCUMENT_PATH will be overridden if
    it already exists

    Example:
    >>> import did
    >>> did.set_documentpath('path2cwd/mydocument')
    >>> did.get_documentpath()
    'path2cwd/mydocument'

    :param path: the file path, which can be both relative to the current working directory
    as well as an absolute path
    """
    dotenv.set_key(ENV, 'DIDDOCUMENTPATH', os.path.abspath(path))


def set_schemapath(path):
    """
    Set the path to the document validation schema, from where the validator will be searching for when
    it tries to validate a json document. SCHEMA_PATH will be overridden if it already exists

    Example:
    >>> import did
    >>> did.set_documentpath('path2cwd/myschema')
    >>> did.get_schemapath()
    'path2cwd/myschema'

    :param path: the file path, which can be both relative to the current working directory as well
    as an absolute path
    """
    dotenv.set_key(ENV, 'DIDSCHEMAPATH', os.path.abspath(path))


def get_documentpath():
    """
    Return the path the DIDDocument, from where the document class will be looking for the
    corresponding .json file when constructing itself.
    """
    return get_variable('DIDDOCUMENTPATH')


def get_schemapath():
    """
    Return the path to the document validation schema, from where the validator will be searching for when
    it tries to validate a json document. SCHEMA_PATH will be overridden if it already exists
    """
    return get_variable('DIDSCHEMAPATH')


def get_db_connection_string(db_name):
    if db_name == 'mongodb':
        return get_variable("MONGO_CONNECTION_STRING")
    elif db_name == 'postgres-sql':
        return get_variable("POSTGRES_CONNECTION_STRING")
    elif db_name == 'gridfs':
        return get_variable("GRIDFS_CONNECTION_STRING")
    else:
        raise AttributeError("{} not supported".format(db_name))


def set_database_connection_string(db_name, connection_string):
    if db_name == 'mongodb':
        dotenv.set_key(ENV, 'MONGO_CONNECTION_STRING', connection_string)
    elif db_name == 'postgres-sql':
        dotenv.set_key(ENV, 'POSTGRES_CONNECTION_STRING', connection_string)
    else:
        raise AttributeError("{} not supported".format(db_name))


def set_variable(key, value):
    """
    Create a global variable that can be accessed by the rest of the program

    Example:
    >>> import did
    >>> settings.globals
    {'CWD' : '', 'DOCUMENT_PATH' : '', SCHEMA_PATH' : ''}
    >>> did.set_variable('USER', 'HELLO')
    >>> settings.globals
    {'CWD' : '', 'DOCUMENT_PATH' : '', 'SCHEMA_PATH' : '', 'USER' : 'HELLO'}

    :param key: 	the global variable name
    :param value:	the global variable value
    :return:
    """
    dotenv.set_key(ENV, key, value)


def get_variable(key):
    """
    Get a global variable from the init.env file in the directory. If the global
    variable is not found in init.env, an Attribute error is raised

    Example:
    >>> import did
    >>> did.list_variables()
    {'CWD' : '', 'DOCUMENT_PATH' : '', SCHEMA_PATH' : '', 'USER', 'HELLO'}
    >>> did.get_variable('USER')
    'HELLO'

    :param key:	the global variable name
    :return:	its corresponding value
    """
    if key not in dotenv.dotenv_values(ENV):
        raise AttributeError('Cannot find global variable {}'.format(key))
    return dotenv.get_key(ENV, key)


def list_variables():
    """
    get all variables and its corresponding values that can be found in init.env

    :return: key-value pairs of global variables and their value as python dictionary
    """
    return dotenv.dotenv_values(ENV)


def parse_didpath(path):
    """
    Parse a path appears in the DEFINITION field of DID Document

    Example:
    >>> parse_didpath("$DIDDOCUMENTPATH/base.json")
    >>> "path2diddocument/base.json"

    :param path:
    :return:
    """

    def replace(string):
        if string.startswith('$') and string[1:] in list_variables():
            return get_variable(string[1:])
        return string

    newpath = map(replace, path.split(get_variable('FILESEP')))
    return os.path.join(*newpath)
