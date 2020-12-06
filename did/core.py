from __future__ import annotations
import did.types as T
from did.database.binary_collection import BinaryCollection
from did.document import DIDDocument
from did.versioning import hash_document, hash_snapshot, hash_commit
from did.exception import NoChangesToSave
from did.time import current_time


class DID:
    def __init__(self, database, binary_directory, auto_save=False):
        self.database = database
        self.bin = BinaryCollection(binary_directory)
        self.auto_save = auto_save
        self.documents_in_transaction = []

    @property
    def db(self):
        return self.database

    def find(self, query=None, version=None):
        return self.db.find(query=query, commit_hash=version)

    def add(self, document, save=None) -> None:
        with self.db.transaction_handler():
            document.data['base']['versions'].insert(0, self.db.working_snapshot_id)
            hash_ = hash_document(document)
            self.db.add(document, hash_)
            self.db.add_to_snapshot(hash_)
        if save if save is not None else self.auto_save:
            self.save()

    def update(self, document, save=None):
        with self.db.transaction_handler():
            document.data['base']['versions'].insert(0, self.db.working_snapshot_id)
            hash_ = hash_document(document)
            self.db.add(document, hash_)
            self.db.add_to_snapshot(hash_)
            self.db.remove_document_from_snapshot(document)
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

    def find_by_id(self, did_id, version=None):
        return self.db.find_by_id(did_id, commit_hash=version)
    
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
        # create snapshot
        if not self.db.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open to write.')
        document_hashes = self.db.get_working_document_hashes()
        if not document_hashes:
            raise NoChangesToSave('The current snapshot has no changes.')
        snapshot_hash = hash_snapshot(self.db.working_snapshot_id, document_hashes)
        self.db.sign_working_snapshot(snapshot_hash)

        # add commit
        commit_hash = hash_commit(snapshot_hash)
        snapshot_id = self.db.working_snapshot_id
        timestamp = current_time()
        current_ref = self.db.current_ref
        previous_commit_hash = current_ref.commit_hash if current_ref else None
        self.db.add_commit(commit_hash, snapshot_id, timestamp, parent=previous_commit_hash)
        self.db.upsert_ref('CURRENT', commit_hash)

        # close transaction
        self.db.save()

    def revert(self):
        """Revert database to point of last save"""
        self.db.revert()
