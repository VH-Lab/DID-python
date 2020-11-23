"""
Database Utils Module
*********************
"""
from __future__ import annotations
import did.types as T
from functools import wraps
from contextlib import contextmanager
from ..query import Query


def listify(func: T.Callable) -> T.Callable:
    """.. currentmodule:: ndi.database.sql
    
    Decorator: meant to work with :class:`SQL` methods. Ensures that the first argument passed into the decorated function is a list. If the value is not a list, it is wrapped in one.
    
    :param func:
    :type func: function
    :return: Returns return value of decorated function.
    """
    @wraps(func)
    def decorator(self: T.Self, arg: T.Foo, *args: T.Args, **kwargs: T.Kwargs) -> None:
        if not isinstance(arg, list):
            func(self, [arg], *args, **kwargs)
        else:
            func(self, arg, *args, **kwargs)
    return decorator

def with_update_warning(func):
    """Decorator: meant to work with :class:`SQL` methods. Ensures that every item in the first argument is a valid :term:`DID object`.
    
    :param func:
    :type func: function
    :return: Returns return value of decorated function.
    """
    @wraps(func)
    def decorator(self, *args, **kwargs):
        if 'force' in kwargs and kwargs['force']:
            func(self, *args, **kwargs)
        else:
            raise RuntimeWarning('Manual updates are strongly discouraged to maintain data integrity across depenencies. To update anyway, use the force argument: db.update(document, force=True).')
    return decorator

def with_delete_warning(func):
    """Decorator: meant to work with :class:`SQL` methods. Ensures that every item in the first argument is a valid :term:`DID object`.
    
    :param func:
    :type func: function
    :return: Returns return value of decorated function.
    """
    @wraps(func)
    def decorator(self, *args, **kwargs):
        if 'force' in kwargs and kwargs['force']:
            func(self, *args, **kwargs)
        else:
            raise RuntimeWarning('Manual deletes are strongly discouraged to maintain data integrity across depenencies. To delete anyway, use the force argument: db.delete(document, force=True).')
    return decorator


"""
SQL Database Specific
=====================
"""

def reduce_ndi_objects_to_ids(ndi_objects):
    try:
        return [obj.id for obj in ndi_objects]
    except TypeError:
        return ndi_objects.id

def recast_ndi_object_to_document(func):
    """Decorator: meant to work with :class:`Collection` methods. Converts a list of :term:`DID object`\ s into their :term:`SQLA document` equivalents.
    
    :param func:
    :type func: function
    :return: Returns return value of decorated function.
    """
    @wraps(func)
    def decorator(self, ndi_object, *args, **kwargs):
        item = self.create_document_from_ndi_object(ndi_object)
        return func(self, item, *args, **kwargs)
    return decorator

def translate_query(func):
    """Decorator: meant to work with :class:`Collection` methods. Converts an :term:`DID query` into an equivalent :term:`SQLA query`.
    
    :param func:
    :type func: function
    :return: Returns return value of decorated function.
    """
    @wraps(func)
    def decorator(self, *args, query=None, sqla_query=None, **kwargs):
        if isinstance(query, Query):
            query = self.generate_sqla_filter(query)
        elif query is None:
            if sqla_query is not None:
                query = sqla_query
            else: pass
        else:
            raise TypeError(f'{query} must be of type Query or CompositeQuery.')
        return func(self, *args, query=query, **kwargs)
    return decorator

def merge_dicts(a, b):
    "merges b into a"
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dicts(a[key], b[key])
            elif a[key] == b[key]:
                pass
            else:
                a[key] = b[key]
        else:
            a[key] = b[key]
    return a