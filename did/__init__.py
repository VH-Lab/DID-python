from .document import DIDDocument
from .query import Query
from .core import DID
from .settings import (
    get_db_connection_string, get_documentpath,
    get_schemapath, set_database_connection_string,
    set_documentpath, set_schemapath, set_variable, list_variables, create_did, set_db_configuration, get_db_configuration, which_config, load_external_configuration)
from .id import DIDId
import warnings

session = None


class __DBNotInitialized:
    def __init__(self, ex=None):
        
        """
        the exception that has occured during the process of instantiating an instance of did.core.DID
        """
        self.error = ex

    def __repr__(self):
        if self.error:
            return "DBNotInitialized('You have not set up a proper default db configuration. Call did.session.error to access the error message')"
        else:
            return "DBNotInitialized(Call db.sessin.init to create database connection)"

    def __str__(self):
        if self.error:
            return "You have not set up a proper default db configuration. Call did.session.error to access the error message"
        else:
            return "Call db.sessin.init() to create database connection"

    def init(self):
        """
        read the DB variable in config.json file, then instantiate an instance of did.core.DID given the option
        set in the DB variable. If successful, it returns an instance of did.core.DID. Otherwise, it raise a warning.
        """
        global session
        try:
            session = create_did()
            return session
        except AttributeError as ex:
            self.error = ex
            warnings.warn(
                "Uncessful initialization. Have you run db.set_db_configuration yet?")
        except Exception as ex:
            self.error = ex
            warnings.warn(
                "Uncessful initialization. Call db.session.error to access the exception")


session = __DBNotInitialized()
