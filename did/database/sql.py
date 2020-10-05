from __future__ import annotations
import ndi.types as T
from enum import Enum
from abc import ABCMeta
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, String, LargeBinary
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import and_, or_
from .ndi_database import NDI_Database
from functools import wraps
from ..document import Document
from ..utils import class_to_collection_name, flatten
from ..decorators import handle_iter, handle_list
from ..query import Query, AndQuery, OrQuery, CompositeQuery
from .utils import listify, with_update_warning, with_delete_warning, update_flatbuffer, recast_ndi_object_to_document, translate_query, with_session, with_open_session, reduce_ndi_objects_to_ids


# ============== #
#  Constants     #
# ============== #

DOCUMENTS_TABLENAME = 'documents'
FLATBUFFER_KEY = 'flatbuffer'

class Datatype(Enum):
    """This enum describes the 3 types of structure a document may exist as in the :class:`SQL` database class."""
    NDI = 2
    SQL_ALCHEMY = 1
    FLATBUFFER = 0

# ============== #
#  SQL Database  #
# ============== #

class SQL(NDI_Database):
    """Interface for SQL Databases.

    .. currentmodule:: ndi.database.ndi_database

    Inherits from the :class:`NDI_Database` abstract class.

    This class aims to manage high-level database operations using the sqlalchemy (SQLA) library and serve as an interface between the NDI and SQLA systems. It maintains the SQLA engine and Session generator, directs :term:`CRUD` actions to their target :class:`Collection`\ s in the database, and is responsible for setting up, configuring, and destroying collections.

    It is closely tied to its :class:`Collection` class, which handles low-level SQLA operations like CRUD implementations. Decorators on the Collection methods are where most of the translation between NDI and SQLA objects occurs (:term:`NDI object` -> :term:`SQLA document`, :term:`NDI query` -> :term:`SQLA query`, etc.). Each :term:`NDI class` translates to a Collection instance.
    """

    _collections: T.SqlDatabaseCollections = {}
    relationships: T.RelationshipMap = {}

    def __init__(self, connection_string: str, mock_session=None) -> None:
        """Sets up a SQL database with collections, binds a sqlAlchemy sessionmaker, and instantiates a slqAlchemy metadata Base.

        :param connection_string: A standard SQL Server connection string.
        :type connection_string: str
        """
        if mock_session:
            self.Session = mock_session
        else:
            self.db = create_engine(connection_string)
            self.Session = sessionmaker(bind=self.db)
        
        self.Base = declarative_base()
        self.__create_collection()

    @with_open_session
    def _sqla_open_session(self, session: T.Session) -> T.Session:
        """Exposes a session for use in a :code:`'with'` statement.
        ::
            with db._sqla_open_session() as session:
                # session operations

        This method handles session setup, commit, and close. Only for use by developers wanting to access the underlying sqlAlchemy layer.

        :param session: Recieved from decorator.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :return: A sqlAlchemy session instance.
        :rtype: Iterator[:class:`sqlalchemy.orm.session.Session`]
        """
        return session

    def execute(self, query: str):
        """Runs a `sqlAlchemy query <https://docs.sqlalchemy.org/en/13/core/connections.html>`_. Only for use by developers wanting to access the underlying sqlAlchemy layer.

        :param query: A SQL query in the format of the given database.
        :type query: str
        """
        return self.db.execute(query)

    def _documents_collection_config(self) -> T.SqlNdiCollectionConfig:
        return {
            'id': Column(String, primary_key=True),
            FLATBUFFER_KEY: Column(LargeBinary),
            'data': Column(JSONB)
        }

    def __create_collection(self) -> None:
        table_name = DOCUMENTS_TABLENAME
        table_fields = self._documents_collection_config()

        self._collections[table_name] = Collection(
            self, self.Base, self.Session, table_name, table_fields, Document
        )

        if hasattr(self, 'db'): # is None when db is mocked with alchemy-mock    
            self.Base.metadata.create_all(self.db)


    def get_tables(self) -> T.Dict[str, T.DeclarativeMeta]:
        """Gets the :term:`SQLA table`\ s in the database.

        .. currentmodule:: ndi.database.sql

        .. note::
           This bypasses the :class:`Collection` layer and is only useful for very specific operations involving the sqlalchemy layer.

        :return: NDI class keys and their :term:`SQLA table` values.
        :rtype: dict
        """
        return {table_name: collection.table for table_name, collection in self._collections.items()}

    def get_table(self, table_name: str) -> T.DeclarativeMeta:
        """Get sql alchemy table object

        :param table_name: [description]
        :type table_name: str
        :return: [description]
        :rtype: T.DeclarativeMeta
        """
        collection = self._collections[table_name]
        return collection.table

    def _get_collection(self, table_name: str) -> T.SqlCollection:
        """.. currentmodule:: ndi.database.sql

        Gets the ndi :class:`Collection` instance of a table.

        param table_name
        type table_name: str
        :rtype: :class:`Collection`
        """
        return self._collections[table_name]

    def __extract_fields(self, ndi_document: T.Document) -> T.SqlCollectionDocument:
        return { key: getattr(ndi_document, key) for key in self._documents_collection_config().keys()}

    def add(self, ndi_document: T.Document) -> None:
        self._collections[DOCUMENTS_TABLENAME].add(ndi_document)
        ndi_document.set_ctx_database(self)

    @with_update_warning
    def update(self, ndi_document: T.Document, force: bool = False) -> None:
        self._collections[DOCUMENTS_TABLENAME].update(ndi_document)

    @with_update_warning
    def upsert(self, ndi_document: T.Document, force: bool = False) -> None:
        self._collections[DOCUMENTS_TABLENAME].upsert(ndi_document)

    @with_delete_warning
    def delete(self, ndi_document: T.Document, force:bool = False) -> None:
        self._collections[DOCUMENTS_TABLENAME].delete(ndi_document)

    def find(self, ndi_query: T.Query = None) -> T.List[T.Document]:
        items = self._collections[DOCUMENTS_TABLENAME].find(
            query=ndi_query,
            result_format=Datatype.NDI
        )
        return [item.with_ctx_database(self) for item in items]

    @with_update_warning
    def update_many(self, ndi_query: T.Query = None, payload: T.SqlCollectionDocument = {}, force: bool = False) -> None:
        self._collections[DOCUMENTS_TABLENAME].update_many(
            self._collections, query=ndi_query, payload=payload)

    @with_delete_warning
    def delete_many(self, query: T.SqlaFilter = None, force:bool = False) -> None:
        self._collections[DOCUMENTS_TABLENAME].delete_many(query=query)

    def find_by_id(self, id_: T.NdiId) -> T.Document:
        item = self._collections[DOCUMENTS_TABLENAME].find_by_id(
            id_,
            result_format=Datatype.NDI    
        )
        return item and item.with_ctx_database(self)

    @with_update_warning
    def update_by_id(self, id_: T.NdiId, payload: T.SqlCollectionDocument = {}, force: bool = False) -> None:
        """Updates the :term:`NDI object` with the given id from the specified :term:`collection` with the fields/values in the :term:`payload`. Fields that aren't included in the payload are not touched.

        .. currentmodule:: ndi.ndi_database

        :param id_: The identifier of the :term:`document` to update.
        :type id_: str
        :param payload: Field and update values to be updated, defaults to {}
        :type payload: :term:`payload`, optional
        """
        self._collections[DOCUMENTS_TABLENAME].update_by_id(
            id_, payload=payload)

    @with_delete_warning
    def delete_by_id(self, id_: T.NdiId, force:bool = False) -> None:
        """Deletes the :term:`NDI object` with the given id from the specified :term:`collection`.

        .. currentmodule:: ndi.ndi_database

        :param ndi_class: The :term:`NDI class` that defines the :term:`collection` to query.
        :type ndi_class: :class:`NDI_Database`
        :param id_: The identifier of the :term:`document` to delete.
        :type id_: str
        """
        self._collections[DOCUMENTS_TABLENAME].delete_by_id(id_)


# ============ #
#  Collection  #
# ============ #

class Collection:
    """.. ndi.database.sql

    Collection class for :class:`SQL` database. :term:`CRUD` operations on this collection are passed on to is corresponding table in the database.

    This class and its associated decorators attempt to encapsulate as much of the sqlalchemy logic as possible (not including database level systems like the Session and engine objects).

    Decorators on the Collection methods are where most of the translation between NDI and SQLA objects occurs (:term:`NDI object` -> :term:`SQLA document`, :term:`NDI query` -> :term:`SQLA query`, etc.). 

    Each Collection instance must have a corresponding :term:`NDI class`.
    """

    def __init__(
        self,
        db: T.Engine,
        Base: T.DeclarativeMeta,
        Session: T.Session,
        table_name: str,
        fields: T.Dict[str, T.Column],
        ndi_class: T.NdiClass = None
    ) -> None:
        """Initializes a :class:`SQL` :class:`Collection`.

        :param Base: `SQLA Metadata Base <https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/api.html#api-reference>`_
        :type Base: :class:`sqlalchemy.ext.declarative.api.DeclarativeMeta`
        :param Session: `SQLA Session <https://docs.sqlalchemy.org/en/13/orm/session.html>`_
        :type Session: :class:`sqlalchemy.orm.session.sessionmaker`
        :param table_name: 
        :type table_name: str, optional
        :param fields: 
        :type fields:
        """
        self.db = db
        self.Session = Session
        self.ndi_class = ndi_class
        self.table_name = table_name
        self.table: T.DeclarativeMeta = type(self.table_name, (Base,), {
            '__tablename__': self.table_name,
            **fields
        })
        self.fields = fields
        self.relationships: T.List[T.SqlRelationship] = []

    def create_document(self, fields: T.Dict[str, T.Column]) -> T.SqlaDocument:
        """Create a :term:`SQLA document` with its respective fields/values

        .. currentmodule:: ndi.database.sql

        :param fields: Contains key:value pairs representing the documents table values per column.
        :type fields: dict
        :return: A :term:`SQLA document`.
        :rtype: :class:`sqlalchemy.ext.declarative.api.DeclarativeMeta`
        """
        return self.table(**fields)

    def create_document_from_ndi_object(self, ndi_object: T.NdiObject) -> T.SqlaDocument:
        """Converts an :term:`NDI object` to a :term:`SQLA document`.

        :param ndi_object:
        :type ndi_object: :term:`NDI class`
        :return: A :term:`SQLA document`.
        :rtype: :class:`sqlalchemy.ext.declarative.api.DeclarativeMeta`
        """
        metadata_fields = {
            key: getattr(ndi_object, key)
            for key in self.fields
            if key != FLATBUFFER_KEY
        }
        fields = {
            FLATBUFFER_KEY: ndi_object.serialize(),
            **metadata_fields,
        }
        return self.create_document(fields)

    @handle_list
    def normalize_documents(
        self,
        document: T.SqlaDocument
    ) -> T.SqlCollectionDocument:
        """Convert one or many :term:`SQLA document`\ s into ``dict``s.

        :param document: Documents as they are recieved from SQLA database operations.
        :type document: List<:term:`SQLA document`> | :term:`SQLA document`
        :return: Documents simplified to ``dict``s, with only Columns/values kept.
        :rtype: List<dict> | dict
        """
        fields = {
            key: getattr(document, key)
            for key in self.fields
        }
        return fields

    @handle_list
    def extract_flatbuffers(self, document: T.SqlaDocument) -> bytearray:
        """Extract the flatbuffer(s) of one or many :term:`SQLA document`\ s.

        :param document: Documents as they are recieved from SQLA database operations.
        :type document:  List<:term:`SQLA document`> | :term:`SQLA document`
        :return: Document's flatbuffer serialization.
        :rtype: List<bytearray> | bytearray
        """
        return getattr(document, FLATBUFFER_KEY)

    @handle_list
    def ndi_objects_from_documents(self, document: T.SqlaDocument) -> T.NdiObject:
        """Creates :term:`NDI object`\ s from documents and rehydrates relationships defined in secondary (lookup) tables.
        
        :param document: 
        :type document: T.SqlaDocument
        :raises TypeError: Thrown when called on a lookup-table collection.
        :rtype: T.NdiObject
        """
        flatbuffer = self.extract_flatbuffers(document)
        if not self.ndi_class or isinstance(self.ndi_class, str):
            raise TypeError(
                'This method may only be used on a collection with an associated NDI class. Lookup tables do not have NDI object equivalents.')
        if isinstance(flatbuffer, bytearray):
            pass
        elif isinstance(flatbuffer, bytes):
            flatbuffer = bytearray(flatbuffer)
        else:
            raise TypeError(f'Unexpected type {type(flatbuffer)} for field {FLATBUFFER_KEY}. Expected bytearray.')
        return self.ndi_class.from_flatbuffer(flatbuffer)

    def __formatted_results(self, documents: T.SqlaDocument, result_format: T.DatatypeEnum) -> T.OneOrManySqlDatabaseDatatype:
        """Formats the given documents as either :term:`NDI object`\ s, :term:`SQLA document`\ s, or :term:`flatbuffer`\ s.
        
        .. currentmodule:: ndi.database.sql

        :param documents: 
        :type documents: T.SqlaDocument
        :param result_format: 
        :type result_format: Datatype
        :rtype: T.OneOrManySqlDatabaseDatatype
        """
        if result_format is Datatype.SQL_ALCHEMY:
            return self.normalize_documents(documents)
        elif result_format is Datatype.FLATBUFFER:
            return self.extract_flatbuffers(documents)
        else:
            return self.ndi_objects_from_documents(documents)

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

    def generate_sqla_filter(self, query: T.Query) -> T.Callable[[T.Query], T.SqlaFilter]:
        """Convert an :term:`NDI query` to a :term:`SQLA query`.

        :param query:
        :type query: :term:`NDI query`
        :return:
        :rtype: :term:`SQLA query`
        """
        def recurse(q: T.Query) -> T.SqlaFilter:
            if isinstance(q, CompositeQuery):
                nested_queries = [recurse(nested_q) for nested_q in q]
                return self._sqla_filter_ops[type(q)](nested_queries)
            else:
                field, operator, value = q.query
                column = self.fields['data'][tuple(field.split('.'))].astext
                return self._sqla_filter_ops[operator](column, str(value))
        return recurse(query)

    def _functionalize_query(self, query: T.SqlaFilter) -> T.Callable[[T.SqlaQuery], T.SqlaQuery]:
        """This function allows us to conditionally set a filter operation on a :term:`SQLA session query`. This allows us to interpret the absence of an :term:`NDI query` or :term:`SQLA query` as being equivalent to ``SELECT *`` within the queried table.

        :param query:
        :type query: :term:`SQLA query` | None
        :return: A :term:`SQLA session query`.
        :rtype: sqlalchemy.orm.query.Query
        """
        def filter_(expr: T.SqlaQuery) -> T.SqlaQuery:
            return expr if query is None else expr.filter(query)
        return filter_

    @recast_ndi_object_to_document
    @with_session
    def add(self, session: T.Session, item: T.SqlaDocument) -> None:
        """Adds :term:`NDI object`\ s to the :class:`Collection` as :term:`SQLA document`\ s.

        .. currentmodule: ndi.database

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param item: Argument is passed in as :term:`NDI object`\ s and is converted to :term:`SQLA document`\ s by :func:`utils.recast_ndi_object_to_document`.
        :type item: List<:term:`SQLA document`>
        """
        session.add(item)

    @recast_ndi_object_to_document
    @with_session
    def update(self, session: T.Session, item: T.SqlaDocument) -> None:
        """Updates (replaces) the :term:`document`\ s of the given :term:`NDI object`\ s in the :class:`Collection` based on the object's id.

        .. currentmodule: ndi.database

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param item: Argument is passed in as :term:`NDI object`\ s and is converted to :term:`SQLA document`\ s by :func:`utils.recast_ndi_object_to_document`.
        :type item: List<:term:`SQLA document`>
        """
        q = session.query(self.table)
        doc = q.get(item.id)
        for key in self.fields:
            setattr(doc, key, getattr(item, key))

    @recast_ndi_object_to_document
    @with_session
    def upsert(self, session: T.Session, item: T.SqlaDocument) -> None:
        """Updates (replaces) the :term:`document`\ s of the given :term:`NDI object`\ s in the :class:`Collection`. Objects that do not already exist are added.

        .. currentmodule: ndi.database

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param item: Argument is passed in as :term:`NDI object`\ s and is converted to :term:`SQLA document`\ s by :func:`utils.recast_ndi_object_to_document`.
        :type item: List<:term:`SQLA document`>
        """
        q = session.query(self.table)
        doc = q.get(item.id)
        if doc is None:
            session.add(item)
        else:
            for key in self.fields:
                setattr(doc, key, getattr(item, key))

    @recast_ndi_object_to_document
    @with_session
    def delete(self, session: T.Session, item: T.SqlaDocument) -> None:
        """Removes the :term:`document`\ s of the given :term:`NDI object`\ s from the :class:`Collection`.

        .. currentmodule: ndi.database

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param item: Argument is passed in as :term:`NDI object`\ s and is converted to :term:`SQLA document`\ s by :func:`utils.recast_ndi_object_to_document`.
        :type item: List<:term:`SQLA document`>
        """
        doc = session.query(self.table).get(item.id)
        session.delete(doc)

    @with_session
    @translate_query
    def find(self, session: T.Session, query: T.SqlaFilter = None, result_format: T.DatatypeEnum = Datatype.NDI) -> T.List[T.SqlDatabaseDatatype]:
        """.. currentmodule:: ndi.database.sql

        Extracts the :term:`document`\ s in the :class:`Collection` matching the given :term:`SQLA query`.

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param query: See :term:`SQLA query`. Defaults to find-all.
        :type query: :class:`sqlalchemy.orm.query.Query`, optional
        :param as_flatbuffers: If False, find will return the results as :term:`SQLA document`\ s as ``dict``s; otherwise, will return :term:`flatbuffers`\ s as ``bytearray``s. Defaults to True.
        :type as_flatbuffers: bool, optional
        :rtype: List<bytearray> | List<dict>
        """
        filter_ = self._functionalize_query(query)
        documents = filter_(session.query(self.table)).all()
        formatted_results = self.__formatted_results(documents, result_format)
        if isinstance(formatted_results, list):
            return formatted_results
        else:
            raise RuntimeError('Unexpected return value from sqlachemy library. If this occurs, it is likely that the library api has changed since the last update.')

    @with_session
    @translate_query
    def delete_many(self, session: T.Session, query: T.SqlaFilter = None) -> None:
        """Deletes the :term:`document`\ s in the :class:`Collection` matching the given :term:`SQLA query`.

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param query: See :term:`SQLA query`. Defaults to find-all.
        :type query: :class:`sqlalchemy.orm.query.Query`, optional
        """
        filter_ = self._functionalize_query(query)
        documents = filter_(session.query(self.table)).all()
        for doc in documents:
            session.delete(doc)

    @with_session
    def find_by_id(self, session: T.Session, id_: T.NdiId, result_format: T.DatatypeEnum = Datatype.NDI) -> T.SqlDatabaseDatatype:
        """Extracts the :term:`document` in the :class:`Collection` matching the given id.

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param id_: See :term:`NDI ID`.
        :type id_: str
        :param as_flatbuffers: If False, find will return the results as :term:`SQLA document`\ s as ``dict``s; otherwise, will return :term:`flatbuffers`\ s as ``bytearray``s. Defaults to True.
        :type as_flatbuffers: bool, optional
        :rtype: bytearray | dict
        """
        document = session.query(self.table).get(id_)
        if not document:
            return document
        formatted_results = self.__formatted_results(document, result_format)
        if not isinstance(formatted_results, list):
            return formatted_results
        else:
            raise RuntimeError('Unexpected return value from sqlachemy library. If this occurs, it is likely that the library api has changed since the last update.')

    @with_session
    def update_by_id(self, session: T.Session, id_: T.NdiId, payload: T.SqlCollectionDocument = {}) -> None:
        """Updates the :term:`document` in the :class:`Collection` matching the given id with payload and returns the updated document flatbuffer.

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param id_: See :term:`NDI ID`.
        :type id_: str
        :param payload: See :term:`payload`.
        :type payload: dict
        :rtype: bytearray
        """
        document = session.query(self.table).get(id_)
        self.__update_sqla_partial_document(document, payload)

    @with_session
    def delete_by_id(self, session: T.Session, id_: T.NdiId) -> None:
        """Deletes the :term:`document` in the :class:`Collection` matching the given id.

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param id_: See :term:`NDI ID`.
        :type id_: str
        """
        document = session.query(self.table).get(id_)
        session.delete(document)

    @with_session
    @translate_query
    def update_many(self, session: T.Session, query: T.SqlaFilter = {}, payload: T.SqlCollectionDocument = {}) -> None:
        """Updates (replaces) the :term:`document`\ s of the given :term:`NDI object`\ s in the :class:`Collection` matching the given :term:`SQLA query`.

        :param session: See :term:`SQLA session`. Argument passed by :func:`utils.with_session`.
        :type session: :class:`sqlalchemy.orm.session.Session`
        :param query: See :term:`SQLA query`. Defaults to find-all.
        :type query: :class:`sqlalchemy.orm.query.Query`, optional
        :param payload: See :term:`payload`.
        :type payload: dict
        :param order_by: :term:`label`. Defaults to last-updated.
        :type order_by: str, optional
        :rtype: List<bytearray>
        """
        filter_ = self._functionalize_query(query)
        documents = filter_(session.query(self.table)).all()
        for d in documents:
            self.__update_sqla_partial_document(d, payload)

    def __update_sqla_partial_document(self, document: T.SqlaDocument, payload: T.SqlCollectionDocument) -> None:
        """Modifies a document in place with the key:value pairs in the given payload.

        :param document:
        :type document: :term:`SQLA document`
        :param payload: See :term:`payload`.
        :type payload: dict
        """
        for key, value in payload.items():
            setattr(document, key, value)
        if hasattr(document, FLATBUFFER_KEY):
            self.__update_sqla_flatbuffer(document, payload)

    def __update_sqla_flatbuffer(self, document: T.SqlaDocument, payload: T.SqlCollectionDocument) -> None:
        """Modifies the flatbuffer of a document with the key:value pairs in the given payload.
        
        :param document: [description]
        :type document: T.SqlaDocument
        :param payload: [description]
        :type payload: T.SqlCollectionDocument
        """
        old_flatbuffer = getattr(document, FLATBUFFER_KEY)
        updated_flatbuffer = update_flatbuffer(
            self.ndi_class, old_flatbuffer, payload)
        setattr(document, FLATBUFFER_KEY, updated_flatbuffer)


