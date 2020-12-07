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

    def add(self, document, save=None) -> None:
        with self.db.transaction_handler():
            if self.db.find_by_id(document.id):
                raise IntegrityError(f'Duplicate Key error for document id={document.id}.')
            document.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
            hash_ = hash_document(document)
            document.data['base']['records'].insert(0, hash_)
            # upsert is used to account for cases where a hash is unchanged due to ignored fields
            self.db.upsert(document, hash_)
            self.db.add_to_snapshot(hash_)
        if save if save is not None else self.auto_save:
            print('saving...')
            self.save()

    def find(self, query=None, snapshot=None, commit=None):
        return self.db.find(query=query, snapshot_id=snapshot, commit_hash=commit)

    def find_by_id(self, did_id, snapshot=None, commit=None):
        return self.db.find_by_id(did_id, snapshot_id=snapshot, commit_hash=commit)

    def find_by_hash(self, document_hash, snapshot=None, commit=None):
        return self.db.find_by_hash(document_hash, snapshot_id=snapshot, commit_hash=commit)

    def update(self, document, save=None):
        with self.db.transaction_handler():
            last_snapshot = document.data['base']['snapshots'][0] if document.data['base']['snapshots'] else None
            previous_hash = document.data['base']['records'][0]
            if last_snapshot == self.db.working_snapshot_id:
                hash_ = hash_document(document)
                document.data['base']['snapshots'][0] = self.db.working_snapshot_id
                document.data['base']['records'][0] = hash_

                self.db.remove_from_snapshot(previous_hash)
                self.db._DANGEROUS__delete_by_hash(previous_hash)
            else:
                document.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
                hash_ = hash_document(document)
                document.data['base']['records'].insert(0, hash_)
                self.db.remove_from_snapshot(previous_hash)

            self.db.upsert(document, hash_)
            self.db.add_to_snapshot(hash_)

        if save if save is not None else self.auto_save:
            self.save()

    def upsert(self, document, save=None):
        with self.db.transaction_handler():
            last_snapshot = document.data['base']['snapshots'][0] if document.data['base']['snapshots'] else None
            previous_hash = document.data['base']['records'][0] if document.data['base']['records'] else None
            if last_snapshot == self.db.working_snapshot_id:
                hash_ = hash_document(document)
                document.data['base']['snapshots'][0] = self.db.working_snapshot_id
                document.data['base']['records'][0] = hash_
                self.db.remove_from_snapshot(previous_hash)
                self.db._DANGEROUS__delete_by_hash(previous_hash)
            else:
                document.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
                hash_ = hash_document(document)
                document.data['base']['records'].insert(0, hash_)
            if previous_hash:
                self.db.remove_from_snapshot(previous_hash)
            self.db.upsert(document, hash_)
            self.db.add_to_snapshot(hash_)
        if save if save is not None else self.auto_save:
            self.save()
    
    def update_by_id(self, did_id, document_updates={}, save=None):
        with self.db.transaction_handler():
            doc = self.db.find_by_id(did_id)
            old_hash = self.db.get_document_hash(doc)
            doc.data = merge_dicts(doc.data, document_updates)
            diff_hash = hash_document(doc)
            if old_hash != diff_hash:
                last_snapshot = doc.data['base']['snapshots'] and doc.data['base']['snapshots'][0]
                if last_snapshot == self.db.working_snapshot_id:
                    doc.data['base']['snapshots'][0] = self.db.working_snapshot_id
                    self.db.remove_from_snapshot(old_hash)
                    self.db._DANGEROUS__delete_by_hash(old_hash)
                else:
                    doc.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
                    self.db.remove_from_snapshot(old_hash)
                hash_ = hash_document(doc)
                doc.data['base']['records'].insert(0, hash_)

                self.db.upsert(doc, hash_)
                self.db.add_to_snapshot(hash_)

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
                    last_snapshot = doc.data['base']['snapshots'] and doc.data['base']['snapshots'][0]
                    if last_snapshot == self.db.working_snapshot_id:
                        doc.data['base']['snapshots'][0] = self.db.working_snapshot_id
                        self.db.remove_from_snapshot(old_hash)
                        self.db._DANGEROUS__delete_by_hash(old_hash)
                    else:
                        doc.data['base']['snapshots'].insert(0, self.db.working_snapshot_id)
                        self.db.remove_from_snapshot(old_hash)
                    hash_ = hash_document(doc)
                    doc.data['base']['records'].insert(0, hash_)

                    self.db.upsert(doc, hash_)
                    self.db.add_to_snapshot(hash_)

        if save if save is not None else self.auto_save:
            self.save()

    def update_dependencies(self, document_hash, dependencies, save=None):
        with self.db.transaction_handler():
            doc = self.db.find_by_hash(document_hash)
            doc.data['dependencies'] = dependencies
            self.db.upsert(doc, document_hash)

        if save if save is not None else self.auto_save:
            self.save()

    def delete(self, document, save=None):
        self.delete_by_id(document.id, save=save)

    def delete_by_id(self, did_id, save=None):
        with self.db.transaction_handler():
            doc = self.db.find_by_id(did_id)
            old_hash = self.db.get_document_hash(doc)
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
        if self.db.current_snapshot == snapshot_hash:
            raise NoChangesToSave('The staged snapshot is equivalent to the previous one.')
        self.db.sign_working_snapshot(snapshot_hash)

        # add commit
        snapshot_id = self.db.working_snapshot_id
        timestamp = current_time()
        current_ref = self.db.current_ref
        previous_commit_hash = current_ref.commit_hash if current_ref else None
        commit_hash = hash_commit(snapshot_hash, snapshot_id, timestamp, previous_commit_hash)
        self.db.add_commit(commit_hash, snapshot_id, timestamp, parent=previous_commit_hash)
        self.db.upsert_ref('CURRENT', commit_hash)

        # close transaction
        self.db.save()

    def revert(self):
        """Revert database to point of last save"""
        self.db.revert()

    def set_current_ref(self, snapshot_id=None, commit_hash=None):
        self.db.set_current_ref(snapshot_id, commit_hash)

    def get_history(self):
        return self.db.get_history()
    
