"""Type Module

Singular location for reusable complex types.
All types are PascalCase.

To use:
::
  from __future__ import annotations
  from typing import TYPE_CHECKING
  if TYPE_CHECKING:
      import ndi.types as T

  Helpful type references: `docs <https://docs.python.org/3/library/typing.html#the-any-type>`_, `examples <https://www.pythonsheets.com/notes/python-typing.html>`_
"""

# common basic types
from typing import TYPE_CHECKING, NewType

NdiId = NewType('NdiId', str)

FilePath = NewType('FilePath', str)

if TYPE_CHECKING:
    from typing import *
    from typing import List, Set, Dict, Tuple, Iterable, Mapping
    # uncommon basic types
    from typing import IO, Pattern, Match, Text
    from typing import Type, ClassVar, Union, Literal, TypedDict
    from typing import Generator, Iterator
    from typing import TypeVar, Callable, Generic, Protocol
    from typing import Optional, Final, Any

    from flatbuffers import Builder
    from abc import ABCMeta

    from .schema.Channel import Channel as Channel_schema
    from .schema.DaqSystem import DaqSystem as DaqSystem_schema
    from .schema.Epoch import Epoch as Epoch_schema
    from .schema.Experiment import Experiment as Experiment_schema
    from .schema.FileNavigator import FileNavigator as FileNavigator_schema
    from .schema.Probe import Probe as Probe_schema
    from .schema.Document import Document as Document_schema

    from .schema.ChannelType import ChannelType as ChannelType_schema
    from .schema.ClockType import ClockType as ClockType_schema
    from .schema.ProbeType import ProbeType as ProbeType_schema

    from ndi.core import NDI_Object
    from ndi.core import Channel, DaqSystem, Epoch, Experiment, Probe, Document, FileNavigator

    from ndi.context import Context
    from ndi.database import SQL, FileSystem
    from ndi.database.sql import Collection as SqlCollection, Datatype as DatatypeEnum
    from ndi.database.file_system import LookupCollection as FS_Lookup_Collection, BinaryCollection
    from ndi.query import Query, CompositeQuery, AndQuery, OrQuery

    from sqlalchemy import Column, Query as SqlaQuery
    from sqlalchemy.orm import relationship, Session
    from sqlalchemy.ext.declarative import DeclarativeMeta
    from sqlalchemy.engine import Engine
    from sqlalchemy.util._collections import _LW as SqlaDocument
    from sqlalchemy.sql.elements import ClauseElement as SqlaFilter 

    """Schema Types
        Associated with the flatbuffer layer in ndi.schema."""

    SchemaClass = Union[
        Type[Channel_schema],
        Type[DaqSystem_schema],
        Type[Epoch_schema],
        Type[Experiment_schema],
        Type[FileNavigator_schema],
        Type[Probe_schema],
        Type[Document_schema]
    ]

    Schema = Union[
        Channel_schema,
        DaqSystem_schema,
        Epoch_schema,
        Experiment_schema,
        FileNavigator_schema,
        Probe_schema,
        Document_schema
    ]

    SchemaVar = TypeVar(
        'SchemaVar',
        Channel_schema,
        DaqSystem_schema,
        Epoch_schema,
        Experiment_schema,
        FileNavigator_schema,
        Probe_schema,
        Document_schema,
    )

    SchemaEnumClass = Union[
        Type[ChannelType_schema],
        Type[ClockType_schema],
        Type[ProbeType_schema]
    ]

    SchemaEnum = Union[
        ChannelType_schema,
        ClockType_schema,
        ProbeType_schema
    ]

    """NDI Types
        Associated with the NDI classes and types."""

    NdiDatabase = Union[SQL, FileSystem]

    NdiClass = Union[
        Type[Experiment],
        Type[Probe],
        Type[DaqSystem],
        Type[Epoch],
        Type[Channel],
        Type[Document],
    ]
    NdiObject = Union[
        Experiment,
        Probe,
        DaqSystem,
        Epoch,
        Channel,
        Document,
    ]
    NdiObjectWithExperimentId = Union[
        Probe,
        DaqSystem,
        Epoch,
        Channel,
    ]
    NdiQueryClass = Union[
        Type[Query],
        Type[AndQuery],
        Type[OrQuery]
    ]

    """Database Types
        Associated with ndi.database drivers."""

    # SQL

    SqlCollectionName = str

    SqlDatabaseCollections = Dict[
        SqlCollectionName,
        SqlCollection
    ]
    RelationshipMap = Dict[NdiClass, relationship]

    SqlRelationshipGenerator = Callable[[SqlDatabaseCollections], relationship]

    SqlNdiCollectionConfig = Dict[str, Union[Column, SqlRelationshipGenerator]]

    SqlLookupTableConfig = SqlNdiCollectionConfig

    SqlDatabaseConfig = Union[
        Dict[NdiClass, SqlNdiCollectionConfig],
        Dict[str, SqlLookupTableConfig],
    ]

    # A SqlaDocument that has been normalized to a Dict that only contains the attributes of its NdiClass.
    SqlCollectionDocument = Dict[str, OneOrManySerializableValue]
    OneOrManySqlCollectionDocument = Union[List[SqlCollectionDocument], SqlCollectionDocument]

    SqlFilterMap = Dict[Union[type, str], Callable[..., SqlaFilter]]


    """Miscellaneous Types / Aliases"""

    SerializableValue = Union[str, int, bytearray, bool]
    OneOrManySerializableValue = Union[List[SerializableValue], SerializableValue]

    BuildOffset = int

    Class = type

    RegexStr = str

    # note that sql.Collection also handles SqlDocuments
    SqlDatabaseDatatype = Union[NdiObject, SqlCollectionDocument, bytearray]
    OneOrManySqlDatabaseDatatype = Union[List[SqlDatabaseDatatype], SqlDatabaseDatatype]

    OneOrManySqlaDocument = Union[List[SqlaDocument], SqlaDocument]
    

    # mostly for use in decorators
    Self = TypeVar('Self')
    Foo = TypeVar('Foo')
    Bar = TypeVar('Bar')
    Baz = TypeVar('Baz')
    OneOrManyFoo = Union[List[Foo], Foo]
    OneOrManyBar = Union[List[Foo], Foo]
    Args = Tuple[Any]
    Kwargs = Dict[Any, Any]

    """Protocols"""
    class WithOpenSessionDecorator(Protocol):
        def __call__(_, self: Self, *args: Args,
                     session: Optional[Session] = None, **kwargs: Kwargs) -> Generator[Session, None, None]: pass


    """TODO"""

    DaqReader = Any
