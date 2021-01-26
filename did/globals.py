import os
import dotenv

# look for init.env in the directory where the script is running
CWD = os.getcwd()
ENV = os.path.join(CWD, '.env')

# if we can find init.env, we create one
if not os.path.isfile(ENV):
	with open(ENV, 'w') as env:
		print('Creating {}'.format(ENV))

__config__ = dotenv.dotenv_values(ENV)

# CWD will serve as a global variable indicating the location the script is running
if 'CWD' not in __config__:
	dotenv.set_key(ENV, 'CWD', CWD)

# check if document_path and schema_path has been override
if 'DIDDOCUMENTPATH' not in __config__:
	dotenv.set_key(ENV, 'DIDDOCUMENTPATH', os.path.join(CWD, 'document'))
if 'DIDSCHEMAPATH' not in __config__:
	dotenv.set_key(ENV, 'DIDSCHEMAPATH', os.path.join(CWD, 'schema'))

DIDDOCUMENTPATH = dotenv.get_key(ENV, 'DIDDOCUMENTPATH')
DIDSCHEMAPATH = dotenv.get_key(ENV, 'DIDSCHEMAPATH')


def set_documentpath(path):
	"""
	Set the path to the DID-document, from where the document class will be looking for the
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
	global DIDDOCUMENTPATH
	dotenv.set_key(ENV, 'DIDDOCUMENTPATH', os.path.abspath(path))
	DIDDOCUMENTPATH = dotenv.get_key(ENV, 'DIDDOCUMENTPATH')


def set_schemapath(path):
	"""
	Set the path to the document schema, from where the validator will be searching for when
	it tries to validate a json document. SCHEMA_PATH will be overridden if it already exists

	Example:
	>>> import did
	>>> did.set_documentpath('path2cwd/myschema')
	>>> did.DIDSCHEMAPATH
	'path2cwd/myschema'

	:param path: the file path, which can be both relative to the current working directory as well
	as an absolute path
	"""
	global DIDSCHEMAPATH
	dotenv.set_key(ENV, 'DIDSCHEMAPATH', os.path.abspath(path))
	DIDSCHEMAPATH = dotenv.get_key(ENV, 'DIDSCHEMAPATH')


def set_variable(key, value):
	"""
	Create a global variable that can be accessed by the rest of the program

	Example:
	>>> import did
	>>> did.get_config()
	{'CWD' : '', 'DOCUMENT_PATH' : '', SCHEMA_PATH' : ''}
	>>> did.set_variable('USER', 'HELLO')
	>>> did.get_config()
	{'CWD' : '', 'DOCUMENT_PATH' : '', 'SCHEMA_PATH' : '', 'USER' : 'HELLO'}

	:param key: 	the global variable name
	:param value:	the global variable value
	:return:
	"""
	if key not in dotenv.dotenv_values(ENV):
		dotenv.set_key(ENV, key, value)


def get_variable(key):
	"""
	Get a global variable from the init.env file in the directory. If the global
	variable is not found in init.env, an Attribute error is raised

	Example:
	>>> import did
	>>> did.get_config()
	{'CWD' : '', 'DOCUMENT_PATH' : '', SCHEMA_PATH' : '', 'USER', 'HELLO'}
	>>> did.get_variable('USER')
	'HELLO'

	:param key:	the global variable name
	:return:	its corresponding value
	"""
	if key not in dotenv.dotenv_values(ENV):
		raise AttributeError('Cannot find global variable {}'.format(key))
	return dotenv.get_key(ENV, key)


def get_config():
	"""
	get all variables and its corresponding values that can be found in init.env

	:return: key-value pairs of global variables and their value as python dictionary
	"""
	return dotenv.dotenv_values(ENV)