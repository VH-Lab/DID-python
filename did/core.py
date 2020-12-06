from __future__ import annotations
import did.types as T
from did.database.binary_collection import BinaryCollection
from did.document import DIDDocument
from did.versioning import hash_document


class DID:
    def __init__(self, database, binary_directory, auto_save=False):
        self.database = database
        self.bin = BinaryCollection(binary_directory)
        self.auto_save = auto_save
        self.documents_in_transaction = []

    @property
    def db(self):
        return self.database

    def find(self, query=None, version=''):
        return self.db.find(query=query)

    def add(self, document, save=None) -> None:
        hash_ = hash_document(document)
        self.db.add(document, hash_)
        self.db.add_to_snapshot(hash_)
        if save if save is not None else self.auto_save:
            self.save()

    def update(self, document, save=None):
        self.db.update(document)
        if save if save is not None else self.auto_save:
            self.save()

    def upsert(self, document, save=None):
        self.db.upsert(document)
        if save if save is not None else self.auto_save:
            self.save()

    def delete(self, document, save=None):
        self.db.delete(document)
        if save if save is not None else self.auto_save:
            self.save()

    def find_by_id(self, did_id, version=''):
        return self.db.find_by_id(did_id)
    
    def update_by_id(self, did_id, document_updates={}, version='', save=None):
        self.db.update_by_id(did_id, updates=document_updates)
        if save if save is not None else self.auto_save:
            self.save()

    def delete_by_id(self, did_id, version='', save=None):
        self.db.delete_by_id(did_id)
        if save if save is not None else self.auto_save:
            self.save()

    def update_many(self, query=None, document_updates={}, version='', save=None):
        self.db.update_many(query=query, updates=document_updates)
        if save if save is not None else self.auto_save:
            self.save()
    
    def delete_many(self, query, version='', save=None):
        self.db.delete_many(query=query)
        if save if save is not None else self.auto_save:
            self.save()

    def save(self):
        # TODO:
        #   hash new version
        #   update version history in database
        #   update new version in affected documents (self.documents_in_transaction)
        #   self.db.commit() again to save version changes
        self.db.save()

    def revert(self):
        """Revert database to point of last save"""
        self.db.revert()
