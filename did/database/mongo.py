import os
import json
from ..globals import get_mongo_connection
from ..versioning import hash_commit, hash_document, hash_snapshot
from ..exception import NoTransactionError, NoWorkingSnapshotError, SnapshotIntegrityError
from .did_driver import DID_Driver
from ..document import DIDDocument
from contextlib import contextmanager
from __future__ import annotations
from typing import TYPE_CHECKING
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId


class MONGODBOptions:
    def __init__(
        self,
        hard_reset_on_init: bool = False,
        debug_mode: bool = False,
        verbose_feedback: bool = True,
    ):
        self.hard_reset_on_init = hard_reset_on_init
        self.debug_mode = debug_mode
        self.verbose_feedback = verbose_feedback

class __WorkingSnapshot:
    def __init__(self, snapshot_id, documents, snapshot_hash):
        self.id2hash = {doc['id'] : doc['hash'] for doc in documents}
        self.hash2id = {self.id2hash[key] : key for key in self.id2hash}
        self.to_be_added = dict()
        self.snapshot_id = snapshot_id
        self.snapshot_hash = snapshot_hash
    
    def add(self, document, hash):
        # update document that already exists with a new hash
        self.id2hash[document.id] = hash
        self.hash2id[hash] = document.id
        self.to_be_added[hash] = document
    
    def delete(self, hash):
        if hash in self.hash2id:
            id = self.hash2id[hash]
            self.id2hash.pop(id, None)
            self.hash2id.pop(hash, None)

    def to_snapshot(self, snapshot_hash):
        document_hashes = [{'id' : id, 'hash' : self.id2hash[id]} for id in self.id2hash]
        return {'type' : 'SNAPSHOT', 'document_hashes' : document_hashes, 'snapshot_hash' : snapshot_hash}


class Mongo(DID_Driver):
    def __init__(
        self, 
        connection_string = get_mongo_connection('raw'),
        hard_reset_on_init = False,
        verbose_feedback = True,
        debug_mode = False):
            self.options = MONGODBOptions(hard_reset_on_init, debug_mode, verbose_feedback)
            self.conn = MongoClient(connection_string)
            try:
                self.conn.admin.command('ismaster')
            except ConnectionFailure:
                raise RuntimeError("Server not available")
            self.db = self.conn['did']
            self.collection = self.db['did_documents']
            self.versioning = self.db['.version']
            self.__current_working_snapshot = None 
            self.__current_session = self.conn.start_session()
        
    def __create_snapshot(self):
        head = self.versioning.find_one({'type' : 'HEAD'})
        if head :
            last_commit = self.versioning.find_one({'type' : 'COMMIT', 'commit_hash' : head['commit_hash']})
            last_snapshot = self.versioning.find_one({'type' : 'SNAPSHOT', 'snapshot_hash' : last_commit['snapshot_hash']})
            return __WorkingSnapshot(last_snapshot['_id'], last_snapshot['document_hashes'], last_snapshot['snapshot_hash'])
        else:
            return self.__setup_version_control()

    def __setup_version_control(self):
        head = {'type' : 'HEAD', 'commit_hash' : None}
        self.versioning.insert_one(head)
        snapshot = {'type' : 'SNAPSHOT', 'document_hashes' : [], 'snapshot_hash' : hash_snapshot([])}
        result = self.versioning.insert_one(snapshot)
        return __WorkingSnapshot(result.inserted_id, snapshot['document_hashes'], snapshot['snapshot_hash'])

    @property
    def working_snapshot_id(self):
        if self.__current_working_snapshot:
            return self.__current_working_snapshot.snapshot_id
        else:
            self.__current_working_snapshot = self.__create_snapshot()
            return self.__current_working_snapshot.snapshot_id
        
    def save(self):
        if self.__current_working_snapshot:
            snapshot = self.__current_working_snapshot.to_snapshot()
            document_to_add = self.__current_working_snapshot.to_be_added
            document_to_add = [{'document_properties' : document_to_add[hash], 'document_hash' : hash} for hash in document_to_add]
            self.versioning.insert_one(snapshot)
            self.collection.insert_many(document_to_add)
            self.__current_working_snapshot = None
            if self.options.verbose_feedback:
                print('Changes saved.')
        else:
            if self.options.verbose_feedback:
                print('No current transactions to save.')
            else:
                raise NoTransactionError('No current transactions to save.')
    
    def revert(self):
        if self.__current_working_snapshot:
            self.__current_working_snapshot = None
        else:
            if self.options.verbose_feedback:
                print('No current transactions to revert.')
            else:
                raise NoTransactionError('No current transactions to revert.')
        
    def transaction_handler(self):
        if self.__current_working_snapshot:
            return self.conn.start_transaction()
        else:
            with self.conn.start_session():
                self.__current_working_snapshot = self.__create_snapshot()
            return self.transaction_handler()
        
    def upsert(self, document, hash_):
        self.current_snapshot.add(document, hash_)

    def find(self, query=None, snapshot_id=None, commit_hash=None, in_all_history=False) -> T.List:
        pass

    def find_by_id(self, id_, snapshot_id=None, commit_hash=None):
        pass

    def find_by_hash(self, document_hash, snapshot_id=None, commit_hash=None):
        pass


    def _DANGEROUS__delete_by_hash(self, hash_) -> None:
        """ 
        Not needed for MongoDB drive because the actual collection is not modified
        """
        return None

    def get_history(self, commit_hash=None):
        if commit_hash:
            return self.versioning.find({'type' : 'COMMIT'})
        else:
            return self.versioning.find({'type' : 'COMMIT', 'commit_hash' : commit_hash})

    @property
    def current_ref(self):
        result = self.versioning.find_one({'type' : 'HEAD'})
        return result['commit_hash']

    @property
    def current_snapshot(self):
        return self.__current_working_snapshot.snapshot_hash
    
    def set_current_ref(self, snapshot_id=None, commit_hash=None):
        """ Sets the CURRENT ref to the given snapshot or commit.
            Given both, commit_hash > snapshot_id.

        :param snapshot_id: defaults to None
        :type snapshot_id: int, optional
        :param commit_hash: defaults to None
        :type commit_hash: str, optional
        :raises RuntimeWarning: [description]
        """
        pass

    def get_commit(self, snapshot_id):
        """ Gets the commit hash associated with the given snapshot.

        :param snapshot_id: A snapshot number
        :type snapshot_id: int
        :raises RuntimeError: Thrown when snapshot_id does not have associated commits.
        :return: commit_hash
        :rtype: str
        """
        pass

    def add_to_snapshot(self, document_hash):
        """ 
        Not needed for MongoDB drive
        """
        return None
    
    def remove_from_snapshot(self, document_hash):
        if not self.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open for modification.')
        self.__current_working_snapshot.delete(document_hash)

    def get_document_hash(self, document):
        if not self.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open.')
        if document.id in self.__current_working_snapshot.id2hash:
            return self.__current_working_snapshot.id2hash[document.id]
        return None

    def get_working_document_hashes(self):
        if not self.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open.')
        return list(self.__current_working_snapshot.hash2id)

    def sign_working_snapshot(self, snapshot_hash):
        if not self.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open.')
        self.__current_working_snapshot = self.__current_working_snapshot.to_snapshot(snapshot_hash)
    
    def add_commit(self, commit_hash, snapshot_id, timestamp, parent=None):
        """Adds a commit to the database.

        :param commit_hash: See did.versioning::hash_commit.
        :type commit_hash: str
        :param snapshot_id: 
        :type snapshot_id: int
        :param timestamp: ISOT. See did.time.
        :type timestamp: str
        :param parent: Parent commit's hash, defaults to None.
        :type parent: str, optional
        """
        pass
        
    
    def upsert_ref(self, name, commit_hash):
        """ Creates a ref if it doesn't already exist.

        :param name: ref name/tag.
        :type name: str
        :param commit_hash: Hash of associated commit.
        :type commit_hash: str
        """
        pass

