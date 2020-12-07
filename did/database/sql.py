from __future__ import annotations
import did.types as T

from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData, ForeignKey, UniqueConstraint
from sqlalchemy import Table, Column, Integer, String, Boolean, Date, DateTime, Interval, Time, type_coerce, cast
from sqlalchemy.dialects.postgresql import JSONB, JSON, insert
from sqlalchemy.sql import select
from sqlalchemy import join, and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy_views import CreateView

from .did_database import DID_Database, DIDDocument
from ..query import Query, AndQuery, OrQuery, CompositeQuery
from ..exception import NoTransactionError, NoWorkingSnapshotError, SnapshotIntegrityError
from .utils import merge_dicts

from contextlib import contextmanager

import datetime

# ============== #
#  SQL Database  #
# ============== #

class SQLOptions:
    def __init__(
        self,
        hard_reset_on_init: bool = False,
        debug_mode: bool = False,
        verbose_feedback: bool = True,
    ):
        self.hard_reset_on_init = hard_reset_on_init
        self.debug_mode = debug_mode
        self.verbose_feedback = verbose_feedback

class SQLTables:
    def __init__(self, ref, commit, snapshot, snapshot_document, document):
        self.ref = ref
        self.commit = commit
        self.snapshot = snapshot
        self.snapshot_document = snapshot_document
        self.document = document
class SQL(DID_Database):
    """"""

    def __init__(
        self, 
        connection_string: str,
        hard_reset_on_init: bool = False,
        debug_mode: bool = False,
        verbose_feedback: bool = True,
    ) -> None:
        """Sets up a SQL database with collections, binds a sqlAlchemy sessionmaker, and instantiates a slqAlchemy metadata Base.

        :param connection_string: A standard SQL Server connection string.
        :type connection_string: str
        """
        self.options = SQLOptions(hard_reset_on_init, debug_mode, verbose_feedback)

        self.db = self._init_database(connection_string)
        self.metadata = MetaData()
        self.table = self._create_tables(self.metadata)
        self.connection: T.Connection = self.db.connect()
        self.current_transaction: T.Optional[T.Transaction] = None
        self.__working_snapshot_id = None

    @property
    def working_snapshot_id(self):
        if not self.__working_snapshot_id:
            self.__working_snapshot_id = self.__create_snapshot()
        return self.__working_snapshot_id

    @working_snapshot_id.setter
    def working_snapshot_id(self, value):
        self.__working_snapshot_id = value

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

        tables = SQLTables(
            Table('ref', metadata,
                Column('name', String, primary_key=True),
                Column('commit_hash', String, ForeignKey('commit.hash'), nullable=False),
                autoload_with=autoload_document_table,
            ),
            Table('commit', metadata,
                Column('hash', String, primary_key=True),
                Column('parent', String, ForeignKey('commit.hash')),
                Column('snapshot_id', Integer, ForeignKey('snapshot.snapshot_id'), nullable=False),
                Column('timestamp', String),
                autoload_with=autoload_document_table,
            ),
            Table('snapshot', metadata, # snapshot + snapshot_documents are analogous to git tree nodes
                Column('snapshot_id', Integer, primary_key=True),
                Column('hash', String),
                autoload_with=autoload_document_table,
            ),
            Table('snapshot_document', metadata,
                Column('snapshot_id', Integer, ForeignKey('snapshot.snapshot_id'), nullable=False),           
                Column('document_hash', String, ForeignKey('document.hash'), nullable=False),
                UniqueConstraint('snapshot_id', 'document_hash')
            ),
            Table('document', metadata, # Analog to git objects
                Column('hash', String, primary_key=True),
                Column('document_id', String, nullable=False),
                Column('data', JSONB, nullable=False),
                autoload_with=autoload_document_table,
            )
        )

        if self.options.hard_reset_on_init or not table_exists:
            metadata.drop_all(self.db, checkfirst=True)
            metadata.create_all(self.db)
        
        return tables

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
        return self.table.document

    def execute(self, query: str):
        """Runs a `sqlAlchemy query <https://docs.sqlalchemy.org/en/13/core/connections.html>`_. Only for use by developers wanting to access the underlying sqlAlchemy layer.

            Note: This query is run outside of the current transaction, so no unsaved changes will be visible.

        :param query: A SQL query in the format of the given database.
        :type query: str
        """
        return self.db.execute(query)

    def save(self):
        if self.current_transaction:
            self.current_transaction.commit()
            self.current_transaction = None
            self.working_snapshot_id = None
            if self.options.verbose_feedback:
                print('Changes saved.')
        else:
            if self.options.verbose_feedback:
                print('No current transactions to save.')
            else:
                raise NoTransactionError('No current transactions to save.')

    def revert(self):
        if self.current_transaction:
            self.current_transaction.rollback()
            self.current_transaction = None
            if self.options.verbose_feedback:
                print('Changes reverted.')
        else:
            if self.options.verbose_feedback:
                print('No current transactions to revert.')
            else:
                raise NoTransactionError('No current transactions to revert.')

    @contextmanager
    def transaction_handler(self, read_only = False) -> T.Generator:
        # TODO: when implementing versioning, will probably need to make this a two-phase transaction in order to do rollbacks on commits within session
        if not self.current_transaction:
            self.current_transaction = self.connection.begin()
            self.working_snapshot_id = self.__create_snapshot()
        yield self.connection

    def add(self, document, hash_) -> None:
        insertion = self.documents.insert().values(
            document_id=document.id,
            data=document.data,
            hash=hash_
        )
        self.connection.execute(insertion)
    
    def upsert(self, document, hash_):
        insertion = insert(self.documents).values(
            document_id=document.id,
            data=document.data,
            hash=hash_
        )
        upsertion = insertion.on_conflict_do_update(
            index_elements=['hash'],
            set_=dict({
                c.name: c
                for c in insertion.excluded
            })
        )
        self.connection.execute(upsertion)

    def find(self, query=None, snapshot_id=None, commit_hash=None, in_all_history=False) -> T.List:
        s = self.select_documents(snapshot_id, commit_hash, in_all_history)
        if query:
            filter_ = self.generate_sqla_filter(query) 
            s = s.where(filter_)
        rows = self.connection.execute(s).fetchall()
        return [self._did_doc_from_row(r) for r in rows]

    def find_by_id(self, id_, snapshot_id=None, commit_hash=None, in_all_history=False):
        s = self.select_documents(snapshot_id, commit_hash, in_all_history) \
            .where(self.documents.c.document_id == id_)
        rows = self.connection.execute(s)
        try:
            return self._did_doc_from_row(next(rows))
        except StopIteration:
            return None

    def find_by_hash(self, document_hash, snapshot_id=None, commit_hash=None, in_all_history=False):
        s = self.select_documents(snapshot_id, commit_hash, in_all_history) \
            .where(self.documents.c.hash == document_hash)
        rows = self.connection.execute(s)
        try:
            return self._did_doc_from_row(next(rows))
        except StopIteration:
            return None

    def _DANGEROUS__delete(self, document) -> None:
        """WARNING: This method modifies the database without version support. Usage of this method may break your database history."""
        delete = self.documents.delete() \
            .where(self.documents.c.document_id == document.id)
        with self.transaction_handler() as connection:
            connection.execute(delete)

    def _DANGEROUS__delete_by_id(self, id_) -> None:
        """WARNING: This method modifies the database without version support. Usage of this method may break your database history."""
        delete = self.documents.delete() \
            .where(self.documents.c.document_id == id_)
        with self.transaction_handler() as connection:
            connection.execute(delete)

    def _DANGEROUS__delete_by_hash(self, hash_) -> None:
        """WARNING: This method modifies the database without version support. Usage of this method may break your database history."""
        delete = self.documents.delete() \
            .where(self.documents.c.hash == hash_)
        with self.transaction_handler() as connection:
            connection.execute(delete)

    def _DANGEROUS__delete_many(self, query=None) -> None:
        """WARNING: This method modifies the database without version support. Usage of this method may break your database history."""
        """ Deletes all documents matching query.
              If no query is provided, deletes ALL documents.

        :param query: [description], defaults to None
        :type query: [type], optional
        """
        delete = self.documents.delete()
        if query:
            filter_ = self.generate_sqla_filter(query) 
            delete = delete.where(filter_)
        with self.transaction_handler() as connection:
            connection.execute(delete)

    def _did_doc_from_row(self, row):
        try:
            doc_id = row['document_id']
            return DIDDocument(row['data'])
        except:
            raise Exception(f'Failure to load DID document {doc_id}. Data appears to be corrupted.')

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

    def generate_sqla_filter(self, query: T.Query):
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
                column = self.documents.c.data[tuple(field.split('.'))]
                column = self._cast_column_by_value(column, value)
                return self._sqla_filter_ops[operator](column, value)
        return recurse(query)
    
    def _cast_column_by_value(self, column, value):
        type_ = type(value)
        if type_ is str:
            return column.astext
        elif type_ is int:
            return column.cast(Integer)
        elif type_ is bool:
            return column.cast(Boolean)
        elif type_ is datetime.date:
            return column.cast(Date)
        elif type_ is datetime.datetime:
            return column.cast(DateTime)
        elif type_ is datetime.timedelta:
            return column.cast(Interval)
        elif type_ is datetime.time:
            return column.cast(Time)
        else:
            return column.astext
    
    def get_history(self, commit_hash=None):
        """Returns history from given commit, with each commit including 
        the snapshot_id, commit_hash, timestamp, ref_names:List[str], and depth.
        Ordered from recent first.
        commit_hash defaults to current commit.
        """
        current_ref = self.current_ref
        if not current_ref and not commit_hash:
            return []
        commit_hash = commit_hash or current_ref.commit_hash
        return list(self.execute(f"""
            SELECT * FROM (
                WITH RECURSIVE log(parent, hash, depth) AS (
                    SELECT  
                        NULL::varchar, '{commit_hash}'::varchar, 1
                    UNION
                    SELECT  log.hash, commit.parent, depth + 1
                    FROM log
                    LEFT JOIN commit ON commit.hash = log.hash
                    WHERE log.hash IS NOT NULL
                )
                SELECT DISTINCT ON (commit.hash)
                    snapshot.snapshot_id, commit.hash, commit.timestamp, COALESCE(ref.names, '[]') as names, depth
                FROM log
                JOIN commit on (log.hash = commit.hash)
                JOIN snapshot on (commit.snapshot_id = snapshot.snapshot_id)
                LEFT JOIN LATERAL (
                    SELECT json_agg(ref.name) as names
                    FROM ref
                    WHERE log.hash = ref.commit_hash
                ) ref ON true
                ORDER BY commit.hash, depth
            ) t
            ORDER BY depth
        """))

    @property
    def current_ref(self):
        try:
            return next(self.execute("""
                SELECT * FROM ref WHERE name = 'CURRENT'
            """))
        except StopIteration:
            return None

    @property
    def current_snapshot(self):
        """ Snapshot associated with CURRENT ref.

        Note: not necessarily equivalent to working snapshot.
        """
        try:
            commit_hash = self.current_ref.commit_hash
        except AttributeError:
            return None
        if commit_hash:
            commit_to_snapshot = self.table.commit \
                .join(self.table.snapshot, 
                    self.table.commit.c.snapshot_id == self.table.snapshot.c.snapshot_id)
            get_associated_documents = select([
                self.table.snapshot
            ]) \
                .select_from(commit_to_snapshot) \
                .where(self.table.commit.c.hash == commit_hash)
            try:
                return next(self.connection.execute(get_associated_documents))
            except StopIteration:
                raise RuntimeError('Failed to get snapshot associated with ref.name == "CURRENT".')
    
    def set_current_ref(self, snapshot_id=None, commit_hash=None):
        if snapshot_id and commit_hash:
            raise RuntimeWarning(f'Warning: You are attempting to select document(s) by both snapshot and commit. The given commit {commit_hash} will take precedence over the given snapshot {snapshot_id}.')
        # TODO: add case for ref when branches are implemented
        if commit_hash:
            pass
        elif snapshot_id:
            commit_hash = self.get_commit(snapshot_id).hash
        update = self.table.ref.update().where(self.table.ref.c.name == 'CURRENT').values(commit_hash=commit_hash)
        self.connection.execute(update)

    def get_commit(self, snapshot_id):
        commit_from_snapshot_id = select([self.table.commit.c.hash])\
            .where(self.table.commit.c.snapshot_id == snapshot_id)
        try:
            return next(self.connection.execute(commit_from_snapshot_id))
        except StopIteration:
            raise RuntimeError('This snapshot does not appear to be associated with any commits.')
    
    def select_documents(self, snapshot_id, commit_hash, in_all_history):
        if snapshot_id and commit_hash:
            print(f'Warning: You are attempting to select document(s) by both snapshot and commit. The given snapshot {snapshot_id} will take precedence over the given commit {commit_hash}.')
        if in_all_history:
            return select([self.table.document])
        elif snapshot_id:
            return self.select_documents_from_snapshot(snapshot_id)
        elif commit_hash:
            return self.select_documents_from_commit(commit_hash)
        else:
            return self.select_documents_from_snapshot(self.working_snapshot_id)

    def select_documents_from_commit(self, commit_hash=None):
        """ Defaults to commits associated with CURRENT ref.

        Note: not necessarily equivalent to working snapshot.
        """
        current_ref = self.current_ref
        commit_hash = commit_hash or current_ref and current_ref.commit_hash
        if commit_hash:
            commit__snapshot__snapshot_document__document = self.table.commit \
                .join(self.table.snapshot, 
                    self.table.commit.c.snapshot_id == self.table.snapshot.c.snapshot_id) \
                .join(self.table.snapshot_document, 
                    self.table.snapshot.c.snapshot_id == self.table.snapshot_document.c.snapshot_id) \
                .join(self.table.document, 
                    self.table.snapshot_document.c.document_hash == self.table.document.c.hash)
            return select([self.table.document]) \
                .select_from(commit__snapshot__snapshot_document__document) \
                .where(self.table.commit.c.hash == commit_hash)
        else:
            return select([self.table.document])
            
    def select_documents_from_snapshot(self, snapshot_id):
        snapshot_document__document = self.table.snapshot_document.join(self.table.document, 
            self.table.snapshot_document.c.document_hash == self.table.document.c.hash)
        return select([self.table.document]) \
            .select_from(snapshot_document__document) \
            .where(self.table.snapshot_document.c.snapshot_id == snapshot_id)

    def __create_empty_snapshot(self):
        insert_empty_snapshot = self.table.snapshot.insert()\
            .returning(self.table.snapshot.c.snapshot_id)
        response = next(self.connection.execute(insert_empty_snapshot))
        return response.snapshot_id

    def __create_snapshot(self):
        """ Creates new transaction's working snapshot.
        Contents of working snapshot are equivalent to git objects in staging.

        :return: [description]
        :rtype: [type]
        """
        snapshot_id = self.__create_empty_snapshot()
        if self.current_ref:
            current_documents = self.connection.execute(self.select_documents_from_commit())
            self.connection.execute(self.table.snapshot_document.insert(), 
                [{ 'snapshot_id': snapshot_id, 'document_hash': doc.hash} for doc in current_documents])
        return snapshot_id


    def __check_working_snapshot_is_mutable(self):
        get_working_snapshot = select([self.table.snapshot]) \
            .where(self.table.snapshot.c.snapshot_id == self.working_snapshot_id)
        result = next(self.connection.execute(get_working_snapshot))
        if result.hash:
            raise SnapshotIntegrityError('Hashed snapshots cannot be ')


    def add_to_snapshot(self, document_hash):
        if not self.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open for modification.')
        insert_new_row = self.table.snapshot_document.insert().values(
            snapshot_id=self.working_snapshot_id,
            document_hash=document_hash,
        )
        try:
            self.connection.execute(insert_new_row)
        except IntegrityError:
            pass # simple equivalent to upsert, but with 2 index elements
    
    def remove_from_snapshot(self, document_hash):
        if not self.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open for modification.')
        delete = self.table.snapshot_document.delete().where(and_(
            self.table.snapshot_document.c.document_hash == document_hash, self.table.snapshot_document.c.snapshot_id == self.working_snapshot_id
        ))
        self.connection.execute(delete)

    def get_document_hash(self, document):
        s = self.select_documents_from_commit()\
            .where(self.table.document.c.document_id == document.id)
        doc = self.connection.execute(s).fetchone()
        try:
            return doc.hash
        except AttributeError:
            return None

    def get_working_document_hashes(self):
        get_associated_documents = select([self.table.snapshot_document]) \
            .where(self.table.snapshot_document.c.snapshot_id == self.working_snapshot_id)
        results = self.connection.execute(get_associated_documents)
        return [row.document_hash for row in results]

    def sign_working_snapshot(self, snapshot_hash):
        self.__check_working_snapshot_is_mutable()
        update = self.table.snapshot.update() \
            .where(self.table.snapshot.c.snapshot_id == self.working_snapshot_id) \
            .values(hash = snapshot_hash)
        self.connection.execute(update)
    
    def add_commit(self, commit_hash, snapshot_id, timestamp, parent=None):
        insert_commit = self.table.commit.insert().values(
            hash=commit_hash,
            snapshot_id=snapshot_id,
            timestamp=timestamp,
            parent=parent
        )
        self.connection.execute(insert_commit)
    
    def upsert_ref(self, name, commit_hash):
        insertion = insert(self.table.ref).values(
            name=name,
            commit_hash=commit_hash
        )
        upsertion = insertion.on_conflict_do_update(
            index_elements=['name'],
            set_=dict({
                c.name: c
                for c in insertion.excluded
            })
        )
        self.connection.execute(upsertion)
