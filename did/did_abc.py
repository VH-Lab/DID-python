from __future__ import annotations
import did.types as T

class DID_ABC:
    def __init__(self, driver, binary_directory, auto_save=False):
        """[summary]

        Instantiates attributes:
        - driver: Exposes the DID_Driver.
        - bin: Exposes the BinaryCollection

        :param driver: A specific database implementation, eg. did.database.SQL.
        :type driver: DID_Driver
        :param binary_directory: The file path for the binary collection.
        :type binary_directory: str
        :param auto_save: If True, saves database on each operation, defaults to False.
        :type auto_save: bool, optional
        """
        pass

    @property
    def db(self):
        """
        :return: self.driver
        :rtype: DID_Driver
        """
        pass

    def add(self, document, save=None) -> None:
        """ Handles adding document to current transaction (working snapshot).
        - Updates document's snapshots and records.
        - Hashes document (did.versioning::hash_document).
        - Submits document to current transaction.
        - Adds document to working snapshot.
        - Saves if save and auto_save indicate.

        :type document: DID_Document
        :param save: Override to auto_save. defaults to None
        :type save: bool, optional
        :raises IntegrityError: Thrown if document already exists in working snapshot.
        """
        pass

    def find(self, query=None, snapshot=None, commit=None, in_all_history=False):
        """ Calls DID_Driver.find()."""
        pass

    def find_by_id(self, did_id, snapshot=None, commit=None, in_all_history=False):
        """ Calls DID_Driver.find_by_id()."""
        pass

    def find_by_hash(self, document_hash, snapshot=None, commit=None, in_all_history=False):
        """ Calls DID_Driver.find_by_hash()."""
        pass

    def update(self, document, save=None):
        """ Handles document update in current transaction.
        - Rehashes document in working snapshot.
        - Adds document to transaction.
        - Removes old version of document from transaction.
        - Saves if save and auto_save indicate.

        :type document: DID_Document
        :param save: Override to auto_save. defaults to None
        :type save: bool, optional
        """
        pass

    def upsert(self, document, save=None):
        """ Adds or updates the document in the working snapshot.

        :type document: DID_Document
        :param save: Override to auto_save. defaults to None
        :type save: bool, optional
        """
        pass
    
    def update_by_id(self, did_id, document_updates={}, save=None):
        """ Handles document update in current transaction.
        - Finds document with given id in working snapshot.
        - Applies updates to document.
        - Rehashes document in working snapshot.
        - Adds document to transaction.
        - Removes old version of document from transaction.
        - Saves if save and auto_save indicate.

        :type document: DID_Document
        :param save: Override to auto_save. defaults to None
        :type save: bool, optional
        """
        pass

    def update_many(self, query=None, document_updates={}, save=None):
        """ Handles document update in current transaction.
        - Finds documents matching query in working snapshot.
        - Applies updates to documents.
        - Rehashes documents in working snapshot.
        - Adds documents to transaction.
        - Removes old version of documents from transaction.
        - Saves if save and auto_save indicate.

        :type document: DID_Document
        :param save: Override to auto_save. defaults to None
        :type save: bool, optional
        """
        pass

    def update_dependencies(self, document_hash, dependencies, save=None):
        """ Updates the dependencies of the document with the given hash.

        :type document_hash: str
        :param dependencies: See ndi.document::Document.__format_dep.
        :type dependencies: list
        :param save: Override to auto_save. defaults to None
        :type save: bool, optional
        """
        pass

    def delete(self, document, save=None):
        """ Calls self.delete_by_id(). """

    def delete_by_id(self, did_id, save=None):
        """ Removes document from current transaction (working snapshot):

        :param did_id: document_id
        :type did_id: str
        :param save: Override to auto_save. defaults to None
        :type save: bool, optional
        """
        pass
    
    def delete_many(self, query, save=None):
        """ Removes all documents matching query from current transaction (working snapshot):

        :type query: did.Query
        :param save: Override to auto_save. defaults to None
        :type save: bool, optional
        """
        pass

    def save(self):
        """ Handles versioning and commits changes to the database.
        - hashes snapshot (did.versioning::hash_snapshot)
        - hashes new commit (did.versioning::hash_commit)
        - updates CURRENT ref to new commit
        - commits operations in transaction to database

        :raises NoChangesToSave: [description]
        """
        pass

    def revert(self):
        """ Reverts database to point of last save."""
        pass

    def set_current_ref(self, snapshot_id=None, commit_hash=None):
        """ Calls DID_Driver.set_current_ref()."""
        pass

    def get_history(self):
        """ Returns DID_Driver.set_current_ref()."""
        pass
    
