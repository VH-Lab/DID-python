from __future__ import annotations
import did.types as T

from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy_utils import database_exists, create_database

from .did_database import DID_Database
from ..query import Query, AndQuery, OrQuery, CompositeQuery
from ..exception import NoTransaction

from contextlib import contextmanager

# ============== #
#  SQL Database  #
# ============== #

class SQLOptions:
    def __init__(
        self,
        auto_save: bool = False,
        hard_reset_on_init: bool = False,
        debug_mode: bool = False,
        verbose_feedback: bool = True,
    ):
        self.auto_save = auto_save
        self.hard_reset_on_init = hard_reset_on_init
        self.debug_mode = debug_mode
        self.verbose_feedback = verbose_feedback

class SQL(DID_Database):
    """"""

    def __init__(
        self, 
        connection_string: str,
        auto_save: bool = False,
        hard_reset_on_init: bool = False,
        debug_mode: bool = False,
        verbose_feedback: bool = True,
    ) -> None:
        """Sets up a SQL database with collections, binds a sqlAlchemy sessionmaker, and instantiates a slqAlchemy metadata Base.

        :param connection_string: A standard SQL Server connection string.
        :type connection_string: str
        """
        self.options = SQLOptions(auto_save, hard_reset_on_init, debug_mode, verbose_feedback)

        self.db = self._init_database(connection_string)
        self.metadata = MetaData()
        self.tables = { 'document': None }
        self._create_tables(self.metadata)
        self.connection: T.Optional[T.Connection] = None
        self.current_transaction: T.Optional[T.Transaction] = None

    def _init_database(self, connection_string):
        if not database_exists(connection_string):
            create_database(connection_string)
            self.options.hard_reset_on_init = True

        engine = create_engine(
            connection_string,
            echo = 'debug' if self.options.debug_mode else False
        )
        return engine

    def _create_tables(self, metadata):
        table_exists = self.__check_table_exists('document')
        autoload_document_table = None\
            if self.options.hard_reset_on_init or not table_exists\
            else self.db
        self.tables['document'] = Table('document', metadata,
            Column('document_id', String, primary_key=True),
            Column('data', JSONB, nullable=False),
            autoload_with=autoload_document_table,
        )
        if self.options.hard_reset_on_init or not table_exists:
            metadata.drop_all(self.db, checkfirst=True)
            metadata.create_all(self.db)

    def __check_table_exists(self, table_name):
        results = self.db.execute(f'''
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        ''')
        return next(results)[0]
    
    @property
    def documents(self):
        return self.tables['document']

    def execute(self, query: str):
        """Runs a `sqlAlchemy query <https://docs.sqlalchemy.org/en/13/core/connections.html>`_. Only for use by developers wanting to access the underlying sqlAlchemy layer.

        :param query: A SQL query in the format of the given database.
        :type query: str
        """
        return self.db.execute(query)

    def save(self):
        if self.current_transaction:
            self.current_transaction.commit()
            self.current_transaction = None
            self.connection = None
            if self.options.verbose_feedback:
                print('Changes saved.')
        else:
            if self.options.verbose_feedback:
                print('No current transactions to save.')
            else:
                raise NoTransaction('No current transactions to save.')

    def revert(self):
        if self.current_transaction:
            self.current_transaction.rollback()
            self.current_transaction = None
            self.connection = None
            if self.options.verbose_feedback:
                print('Changes reverted.')
        else:
            if self.options.verbose_feedback:
                print('No current transactions to revert.')
            else:
                raise NoTransaction('No current transactions to revert.')

    @contextmanager
    def transaction_handler(self, save) -> T.Generator:
        # TODO: when implementing versioning, will probably need to make this a two-phase transaction in order to do rollbacks on commits within session
        if not self.current_transaction:
            self.connection = self.db.connect()
            self.current_transaction = self.connection.begin()
        yield self.connection
        if save or self.options.auto_save:
            self.save()

    def find(self, query=None):
        pass

    def add(self, document, save=False) -> None:
        insertion = self.documents.insert().values(
            document_id=document.id,
            data=document.serialize()
        )
        with self.transaction_handler(save) as connection:
            result = connection.execute(insertion)

    def update(self, document, save=False) -> None:
        pass

    def upsert(self, document, save=False) -> None:
        pass

    def delete(self, document, save=False) -> None:
        pass

    def find_by_id(self, id_):
        pass

    def update_by_id(self, id_, updates={}, save=False) -> None:
        pass

    def delete_by_id(self, id_, save=False) -> None:
        pass

    def update_many(self, query=None, updates={}, save=False) -> None:
        pass

    def delete_many(self, query=None, save=False) -> None:
        pass

    _sqla_filter_ops: T.SqlFilterMap = {
        # composite types
        AndQuery: lambda conditions: and_(*conditions),
        OrQuery: lambda conditions: or_(*conditions),
        # operators
        # value must be string (because all queries are on JSON data field)
        '==': lambda field, value: field == value,
        '!=': lambda field, value: field != value,
        'contains': lambda field, value: field.contains(value),
        'match': lambda field, value: field.match(value),
        '>': lambda field, value: field > value,
        '>=': lambda field, value: field >= value,
        '<': lambda field, value: field < value,
        '<=': lambda field, value: field <= value,
        'exists': lambda field, value: field,
        'in': lambda field, value: field.in_(value),
    }

    def generate_sqla_filter(self, document, query: T.Query):
        """Convert an :term:`DID query` to a :term:`SQLA query`.

        :param query:
        :type query: :term:`DID query`
        :return:
        :rtype: :term:`SQLA query`
        """
        def recurse(q: T.Query):
            if isinstance(q, CompositeQuery):
                nested_queries = [recurse(nested_q) for nested_q in q]
                return self._sqla_filter_ops[type(q)](nested_queries)
            else:
                field, operator, value = q.query
                column = document.data[tuple(field.split('.'))].astext
                return self._sqla_filter_ops[operator](column, str(value))
        return recurse(query)

    def _functionalize_query(self, query) -> T.Callable:
        """This function allows us to conditionally set a filter operation on a :term:`SQLA session query`. This allows us to interpret the absence of an :term:`DID query` or :term:`SQLA query` as being equivalent to ``SELECT *`` within the queried table.

        :param query:
        :type query: :term:`SQLA query` | None
        :return: A :term:`SQLA session query`.
        :rtype: sqlalchemy.orm.query.Query
        """
        def filter_(expr):
            return expr if query is None else expr.filter(query)
        return filter_