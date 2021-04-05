from .document import DIDDocument
from .query import Query
from .core import DID
from .settings import (
    get_db_connection_string, get_documentpath,
    get_schemapath, set_database_connection_string,
    set_documentpath, set_schemapath, set_variable, list_variables, create_did, set_db_configuration, get_db_configuration)
from .id import DIDId
import warnings


class __DBNotInitialized:
    def __init__(self, ex):
        self.ex = ex

    def __repr__(self):
        return "DBNotInitialized('You have not set up a proper default db configuration. Call did.session.error to access the error message')"

    def __str__(self):
        return 'You have not set up a proper default db configuration. Call did.session.error to access the error message'

    def retry(self):
        global session
        session = create_did()

    def error(self):
        return self.ex


try:
    session = create_did()
except Exception as ex:
    session = __DBNotInitialized(ex)
