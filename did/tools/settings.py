import os
import dotenv
import did
from urllib.parse import quote_plus


# look for config.env in the directory where the modeul is loaded
INSTALLATION = os.path.split(os.path.abspath(did.__path__[0]))[0]
ENV = os.path.join(INSTALLATION, 'config.env')

# if we can find config.env, we create one
if not os.path.isfile(ENV):
    env = open(ENV, 'w')
    env.close()

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
    dotenv.set_key(ENV, 'MONGO_CONNECTION_STRING', "mongodb://localhost:27017")
if 'POSTGRES_CONNECTION_STRING' not in __config__:
    dotenv.set_key(ENV, 'POSTGRES_CONNECTION_STRING',
                   "postgres://postgres:password@localhost:5432/did_versioning_tests")


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
    >>> did.DIDDOCUMENTPATH
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
    >>> did.DIDSCHEMAPATH
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
    >>> did.globals
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
    >>> parse_didpath("{$DIDDOCUMENTPATH}/base.json")
    >>> "path2diddocument/base.json"

    :param path:
    :return:
    """

    def replace(string):
        if string.startswith('$') and string[1:] in globals():
            return get_variable(string[1:])
        return string

    newpath = map(replace, path.split(get_variable('FILESEP')))
    return os.path.join(*newpath)
