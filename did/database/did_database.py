from __future__ import annotations
import did.types as T
from abc import ABC, abstractmethod

from ..document import DIDDocument

class DID_Database(ABC):
    """
    Abstract class for DID database interfaces.
    Child classes of :class:`DID_Database` are standardized, and share the same base methods and data signatures.
    """
    _collections: T.Dict = {
        DIDDocument: None
    }

    @abstractmethod
    def __init__(self):
        pass

    def commit(self):
        """ This should close whatever transaction is open and commit the new experiment version to the database """
        pass

    def unstage_changes(self):
        """ This should close whatever transaction is open and commit the new experiment version to the database """
        pass

    @abstractmethod
    def add(self, did_document):
        """It should be able to add a :term:`DIDDocument` to the database
        
        :param did_document:
        :type did_document: 
        """
        pass

    @abstractmethod
    def update(self, did_document, force=False):
        """It should be able to update a :term:`DIDDocument object`. Updated entries are found by their id.
        
        :param did_document:
        :type did_document: 
        """
        pass

    @abstractmethod
    def upsert(self, did_document, force=False):
        """It should be able to update one or many instances of an :term:`DIDDocument object`. Updated entries are found by their :term:`DID class` and id. In the case that an instance does not already exist, it should be added to its :term:`collection`.
        
        :param did_document:
        :type did_document: 
        """
        pass

    @abstractmethod
    def delete(self, did_document, force=False):
        """It should be able to delete one or many instances of an :term:`DIDDocument object`.
        
        :param did_document:
        :type did_document: 
        """
        pass

    @abstractmethod
    def find_by_id(self, id_):
        """It should be able to retrieve a single :term:`document` given the :term:`DID class` it belongs to and its id.

        :param ndi_class:
        :type ndi_class: :class:`DID_Object`
        :param id_:
        :type id_: str
        """
        pass

    @abstractmethod
    def update_by_id(self, id_, payload, force=False):
        """It should be able to update a single :term:`document` given the :term:`DID class` it belongs to, its id, and the data being updated.

        :param ndi_class:
        :type ndi_class: :class:`DID_Object`
        :param id_:
        :type id_: str
        :param payload: Containing fields with values to update to in the specified ndi_class. Not passing a dict will result in no updates.
        :type payload: :term:`payload`, optional
        """
        pass

    @abstractmethod
    def delete_by_id(self, id_, force=False):
        """It should be able to remove a single :term:`document` from a collection given its :term:`DID class` and id.

        :param ndi_class:
        :type ndi_class: :class:`DID_Object`
        :param id_:
        :type id_: str
        """
        pass

    @abstractmethod
    def find(self, query):
        """It should be able to utilize a :term:`DID query` to retrive data from the :term:`collection` of the given :term:`DID class`.

        :param query: A set of conditions to find on. Not passing a query will return all :term:`document`\ s from the :term:`collection.
        :type query: :term:`DID query`, optional
        """
        pass

    @abstractmethod
    def update_many(self, query, payload, force=False):
        """It should be able to update all :term:`document`\ s matching the given :term:`DID query` in the :term:`collection` of the given :term:`DID class`.

        :param ndi_class:
        :type ndi_class: :class:`DID_Object`
        :param query: A set of conditions to update on. Not passing a query will update all :term:`document`\ s from the :term:`collection`.
        :type query: :term:`DID query`, optional
        :param payload: Containing fields with values to update to in the specified ndi_class. Not passing a dict will result in no updates.
        :type payload: :term:`payload`, optional
        """
        pass

    @abstractmethod
    def delete_many(self, query, force=False):
        """It should be able to delete all :term:`document`\ s matching the given :term:`DID query` in the :term:`collection` of the given :term:`DID class`.

        :param ndi_class:
        :type ndi_class: :class:`DID_Object`
        :param query: A set of conditions to delete on. Not passing a query will delete all :term:`document`\ s from the :term:`collection`.
        :type query: :term:`DID query`, optional
        """
        pass
