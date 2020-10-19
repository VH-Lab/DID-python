from __future__ import annotations
import ndi.types as T
from abc import ABC, abstractmethod

from ..document import Document

class NDI_Database(ABC):
    """
    Abstract class for NDI database interfaces.
    Child classes of :class:`NDI_Database` are standardized, and share the same base methods and data signatures.
    """
    _collections: T.Dict = {
        Document: None
    }

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def add(self, ndi_document):
        """It should be able to add one or many instances of an :term:`NDI object` to the appropriate :term:`collection`.
        
        :param ndi_document:
        :type ndi_document: :term:`NDI object`
        """
        pass

    @abstractmethod
    def update(self, ndi_document, force=False):
        """It should be able to update one or many instances of an :term:`NDI object`. Updated entries are found by their :term:`NDI class` and id.
        
        :param ndi_document:
        :type ndi_document: :term:`NDI object`
        """
        pass

    @abstractmethod
    def upsert(self, ndi_document, force=False):
        """It should be able to update one or many instances of an :term:`NDI object`. Updated entries are found by their :term:`NDI class` and id. In the case that an instance does not already exist, it should be added to its :term:`collection`.
        
        :param ndi_document:
        :type ndi_document: :term:`NDI object`
        """
        pass

    @abstractmethod
    def delete(self, ndi_document, force=False):
        """It should be able to delete one or many instances of an :term:`NDI object`.
        
        :param ndi_document:
        :type ndi_document: :term:`NDI object`
        """
        pass

    @abstractmethod
    def find_by_id(self, id_):
        """It should be able to retrieve a single :term:`document` given the :term:`NDI class` it belongs to and its id.

        :param ndi_class:
        :type ndi_class: :class:`NDI_Object`
        :param id_:
        :type id_: str
        """
        pass

    @abstractmethod
    def update_by_id(self, id_, payload, force=False):
        """It should be able to update a single :term:`document` given the :term:`NDI class` it belongs to, its id, and the data being updated.

        :param ndi_class:
        :type ndi_class: :class:`NDI_Object`
        :param id_:
        :type id_: str
        :param payload: Containing fields with values to update to in the specified ndi_class. Not passing a dict will result in no updates.
        :type payload: :term:`payload`, optional
        """
        pass

    @abstractmethod
    def delete_by_id(self, id_, force=False):
        """It should be able to remove a single :term:`document` from a collection given its :term:`NDI class` and id.

        :param ndi_class:
        :type ndi_class: :class:`NDI_Object`
        :param id_:
        :type id_: str
        """
        pass

    @abstractmethod
    def find(self, ndi_query):
        """It should be able to utilize a :term:`NDI query` to retrive data from the :term:`collection` of the given :term:`NDI class`.

        :param ndi_query: A set of conditions to find on. Not passing a query will return all :term:`document`\ s from the :term:`collection.
        :type ndi_query: :term:`NDI query`, optional
        """
        pass

    @abstractmethod
    def update_many(self, ndi_query, payload, force=False):
        """It should be able to update all :term:`document`\ s matching the given :term:`NDI query` in the :term:`collection` of the given :term:`NDI class`.

        :param ndi_class:
        :type ndi_class: :class:`NDI_Object`
        :param ndi_query: A set of conditions to update on. Not passing a query will update all :term:`document`\ s from the :term:`collection`.
        :type ndi_query: :term:`NDI query`, optional
        :param payload: Containing fields with values to update to in the specified ndi_class. Not passing a dict will result in no updates.
        :type payload: :term:`payload`, optional
        """
        pass

    @abstractmethod
    def delete_many(self, query, force=False):
        """It should be able to delete all :term:`document`\ s matching the given :term:`NDI query` in the :term:`collection` of the given :term:`NDI class`.

        :param ndi_class:
        :type ndi_class: :class:`NDI_Object`
        :param ndi_query: A set of conditions to delete on. Not passing a query will delete all :term:`document`\ s from the :term:`collection`.
        :type ndi_query: :term:`NDI query`, optional
        """
        pass
