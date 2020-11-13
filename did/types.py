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

    from did import Query, DIDDocument

    from sqlalchemy import Column, Query as SqlaQuery
    from sqlalchemy.orm import relationship, Session
    from sqlalchemy.ext.declarative import DeclarativeMeta
    from sqlalchemy.engine import Engine
    from sqlalchemy.util._collections import _LW as SqlaDocument
    from sqlalchemy.sql.elements import ClauseElement as SqlaFilter 
    from sqlalchemy.engine import Connection, Transaction


    SqlFilterMap = Dict[Union[type, str], Callable[..., SqlaFilter]]