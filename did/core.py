from __future__ import annotations
import did.types as T
from did.database.binary_collection import BinaryCollection
from did.document import DIDDocument
from did.versioning import hash_document, hash_snapshot, hash_commit
from did.exception import NoChangesToSave, NoChangesToSave, IntegrityError
from did.time import current_time
from did.database.utils import merge_dicts


class DID:
    def __init__(self, database, binary_directory, auto_save=False):
        self.database = database
        self.bin = BinaryCollection(binary_directory)
        self.auto_save = auto_save
        self.documents_in_transaction = []

    @property
    def db(self):
        return self.database

    def find(self, query=None, snapshot=None, commit=None):
        return self.db.find(query=query, snapshot_id=snapshot, commit_hash=commit)

    def add(self, document, save=None) -> None:
        with self.db.transaction_handler():
            if self.db.find_by_id(document.id):
                raise IntegrityError(f'Duplicate Key error for document {document.id}')
            document.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
            hash_ = hash_document(document)
            document.data['base']['records'].insert(0, hash_)
            self.db.add(document, hash_)
            self.db.add_to_snapshot(hash_)
        if save if save is not None else self.auto_save:
            print('saving...')
            self.save()

    def update(self, document, save=None):
        with self.db.transaction_handler():
            document.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
            hash_ = hash_document(document)
            document.data['base']['records'].insert(0, hash_)

            self.db.add(document, hash_)
            self.db.add_to_snapshot(hash_)

            old_hash = self.db.get_document_hash(document)
            self.db.remove_from_snapshot(old_hash)
        if save if save is not None else self.auto_save:
            self.save()

    def upsert(self, document, save=None):
        with self.db.transaction_handler():
            old_hash = self.db.get_document_hash(document)
            document.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
            hash_ = hash_document(document)
            document.data['base']['records'].insert(0, hash_)
            self.db.add(document, hash_)
            self.db.add_to_snapshot(hash_)
            if old_hash:
                self.db.remove_from_snapshot(old_hash)
        if save if save is not None else self.auto_save:
            self.save()

    def delete(self, document, save=None):
        self.delete_by_id(document.id, save=save)

    def find_by_id(self, did_id, snapshot=None, commit=None):
        return self.db.find_by_id(did_id, snapshot_id=snapshot, commit_hash=commit)
    
    def update_by_id(self, did_id, document_updates={}, save=None):
        with self.db.transaction_handler():
            doc = self.db.find_by_id(did_id)
            old_hash = self.db.get_document_hash(doc)
            doc.data = merge_dicts(doc.data, document_updates)
            diff_hash = hash_document(doc)
            if old_hash != diff_hash:
                doc.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
                hash_ = hash_document(doc)
                doc.data['base']['records'].insert(0, hash_)

                self.db.add(doc, hash_)
                self.db.add_to_snapshot(hash_)

                self.db.remove_from_snapshot(old_hash)
        if save if save is not None else self.auto_save:
            self.save()

    def delete_by_id(self, did_id, save=None):
        with self.db.transaction_handler():
            doc = self.db.find_by_id(did_id)
            old_hash = self.db.get_document_hash(doc)
            self.db.remove_from_snapshot(old_hash)
        if save if save is not None else self.auto_save:
            self.save()

    def update_many(self, query=None, document_updates={}, save=None):
        with self.db.transaction_handler():
            documents = self.db.find(query=query)
            for doc in documents:
                old_hash = self.db.get_document_hash(doc)
                doc.data = merge_dicts(doc.data, document_updates)
                diff_hash = hash_document(doc)
                if old_hash != diff_hash:
                    doc.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
                    hash_ = hash_document(doc)
                    doc.data['base']['records'].insert(0, hash_)

                    self.db.add(doc, hash_)
                    self.db.add_to_snapshot(hash_)

                    self.db.remove_from_snapshot(old_hash)
        if save if save is not None else self.auto_save:
            self.save()
    
    def delete_many(self, query, save=None):
        with self.db.transaction_handler():
            documents = self.db.find(query=query)
            for doc in documents:
                old_hash = self.db.get_document_hash(doc)
                self.db.remove_from_snapshot(old_hash)
        if save if save is not None else self.auto_save:
            self.save()

    def save(self):
        # create snapshot
        document_hashes = self.db.get_working_document_hashes()
        snapshot_hash = hash_snapshot(document_hashes)
        if self.db.current_snapshot.hash:
            raise NoChangesToSave('The staged snapshot is equivalent to the previous one.')
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

    def get_history(self):
        return self.db.get_history()
