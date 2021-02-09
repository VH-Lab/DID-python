import os
import json
from datetime import datetime as dt
from ..globals import get_mongo_connection
from ..versioning import hash_commit, hash_document, hash_snapshot
from ..exception import NoTransactionError, NoWorkingSnapshotError, SnapshotIntegrityError
from .did_driver import DID_Driver
from ..document import DIDDocument
from contextlib import contextmanager
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from bson.objectid import ObjectId
from pymongo.collection import Collection

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

class _WorkingSnapshot:
    def __init__(self, snapshot_id, documents, snapshot_hash):
        self.id2hash = {doc['id'] : doc['hash'] for doc in documents}
        self.hash2id = {self.id2hash[key] : key for key in self.id2hash}
        self.to_be_added = dict()
        self.snapshot_id = snapshot_id
        self.snapshot_hash = snapshot_hash

    def add_doc(self, document, hash):
        self.to_be_added[hash] = document
    
    def add_hash(self, document, hash):
        # update document that already exists with a new hash, document_id and hash
        # has one-on-one relationship
        self.id2hash[document.id] = hash
        self.hash2id[hash] = document.id
        
    def delete(self, hash):
        if hash in self.hash2id:
            id = self.hash2id[hash]
            self.id2hash.pop(id, None)
            self.hash2id.pop(hash, None)

    def to_snapshot(self, snapshot_hash):
        document_hashes = [{'id' : id, 'hash' : self.id2hash[id]} for id in self.id2hash]
        return {'type' : 'SNAPSHOT', 'document_hashes' : document_hashes, 'snapshot_hash' : snapshot_hash}


class _TransactionHandler:
    def __init__(self):
        self.actions = []
        self.already_executed = []
        
    def __enter__(self):
        return None
    
    def action_on(self, 
        instance, 
        callback, 
        args_for_callback, 
        reverse_callback_success, 
        args_for_reverse_callback_success, 
        reverse_callback_fail=None, 
        args_for_reverse_callback_fail=None):            
            self.actions.append({'instance' : instance,
                                'callback' : (callback, args_for_callback),
                                'reverse_callback_success': (reverse_callback_success, args_for_reverse_callback_success),
                                'reverse_callback_fail' : (reverse_callback_fail, args_for_reverse_callback_fail),
                                'success' : False, 
                                'return_value' : None})

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None and exc_value is None and traceback is None:
            try:
                for action in self.actions:
                    self.already_executed.append(action)
                    instance = action['instance'] if 'instance' in action else None
                    callback, args = action['callback']
                    returned_value = None
                    if args and isinstance(args, type(0)):
                        args = self.actions[args]['return_value']
                        if not isinstance(args, type([])):
                            args = [args]
                    if instance and args:
                        returned_value = callback(instance, *list(args))
                    elif instance:
                        returned_value = callback(instance)
                    elif args:
                        returned_value = callback(*list(args))
                    else:
                        returned_value = callback()
                    action['success'] = True
                    action['return_value'] = returned_value
            except Exception as e:
                for action in reversed(self.already_executed):
                    instance = action['instance'] if 'instance' in action else None
                    if action['success']:
                        reverse_callback, args = action['reverse_callback_success']
                        if callback:
                            if instance and args:
                                reverse_callback(instance, *list(args))
                            elif instance:
                                reverse_callback(instance)
                            elif args:
                                reverse_callback(*list(args))
                            else:
                                reverse_callback()
                    else:
                            reverse_callback, args = action['reverse_callback_fail']
                            if reverse_callback:
                                if instance and args:
                                    reverse_callback(instance, *list(args))
                                elif instance:
                                    reverse_callback(instance)
                                elif args:
                                    reverse_callback(*list(args))
                                else:
                                    reverse_callback()
                raise e
        else:
            raise RuntimeError("Exception occured: type: {}, value: {}, traceback: {}".format(exc_type, exc_value, traceback))

class Mongo(DID_Driver):
    def __init__(
        self, 
        connection_string = None,
        hard_reset_on_init = False,
        verbose_feedback = True,
        debug_mode = False):

            def __make_connection(connection_string):
                try:
                    client = MongoClient(connection_string)
                    client.server_info()
                    return client
                except ServerSelectionTimeoutError:
                    raise ConnectionError("Fail the connect to the database @{}".format(connection_string))

            if connection_string is None:
                connection_string = get_mongo_connection('raw')
            self.options = MONGODBOptions(hard_reset_on_init, debug_mode, verbose_feedback)
            self.conn = __make_connection(connection_string)
            self.db = self.conn['did']
            self.collection = self.db['did_documents']
            self.versioning = self.db['version']
            self.__current_working_snapshot = None 
            self.__current_session = self.conn.start_session()
        
    def __create_snapshot(self):
        head = self.versioning.find_one({'type' : 'HEAD'})
        if head:
            last_commit = self.versioning.find_one({'type' : 'COMMIT', 'commit_hash' : head['commit_hash']})
            last_snapshot = self.versioning.find_one({'type' : 'SNAPSHOT', 'snapshot_hash' : last_commit['snapshot_hash']})
            return _WorkingSnapshot(last_snapshot['_id'], last_snapshot['document_hashes'], last_snapshot['snapshot_hash'])
        else:
            self.__setup_version_control()

    def __setup_version_control(self):
        self.__current_session = _TransactionHandler()
        with self.__current_session:
            head = {'type' : 'HEAD', 'commit_hash' : None}
            self.__current_session.action_on(self.versioning, Collection.insert_one, [head], Collection.delete_one, [{'type' : 'HEAD'}])
            snapshot = {'type' : 'SNAPSHOT', 'document_hashes' : [], 'snapshot_hash' : hash_snapshot([]), 'commit_hahses': []}
            self.__current_session.action_on(self.versioning, Collection.insert_one, [snapshot], Collection.delete_one, [{'type' : 'SNAPSHOT'}])
            timestamp = dt.now().isoformat()
            
            def make_commit(id):
                id = id.inserted_id
                return {'type' : 'COMMIT',
                        'commit_hash' : hash_commit(hash_snapshot([]), str(id), timestamp),
                        'snapshot_id' : id,
                        'timestamp' : timestamp,
                        'message': 'database initialized',
                        'parent' : None, }
            
            self.__current_session.action_on(None, make_commit, -3, None, None)
            self.__current_session.action_on(self.versioning, Collection.insert_one, 
                    -2, Collection.delete_one, [{'type' : 'COMMIT'}])
        return _WorkingSnapshot(id, snapshot['document_hashes'], snapshot['snapshot_hash'])

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
            self.__current_session = _TransactionHandler()
            return self.__current_session
        else:
            with self.conn.start_session():
                self.__current_working_snapshot = self.__create_snapshot()
            return self.transaction_handler()
        
    def upsert(self, document, hash_):
        if self.__current_working_snapshot:
            doc = self.find_by_hash(hash_)
            if doc is None:
                self.__current_working_snapshot.add_doc(document, hash_)
            self.__current_working_snapshot.add_hash(document, hash_)
        else:
            if self.options.verbose_feedback:
                print('No current transactions to revert.')
            else:
                raise NoTransactionError('No current transactions to revert.')
     
    def find(self, query=None, snapshot_id=None, commit_hash=None, in_all_history=False):
        #TODO implement find in those three scenarios 
        if snapshot_id and commit_hash or snapshot_id:
            return None
        elif commit_hash:
            return None
        else:
            return None

    def find_by_id(self, id_, snapshot_id=None, commit_hash=None):
        def find_doc_from_snapshot(snapshot, id_):
            if id_ not in snapshot['id2hashes']:
                    return None
            else:
                hash = snapshot['id2hashes'][id_]
                return self.collection.find_one({'base.record' : hash})
        if snapshot_id and commit_hash or snapshot_id:
            if snapshot_id == self.current_snapshot:
                return self.find_by_id(id_)
            else:
                snapshot = self.versioning.find_one({'type' : 'SNAPSHOT', '_id' : ObjectId(snapshot_id)})
                return find_doc_from_snapshot(snapshot, id_)
        elif commit_hash:
            commit = self.versioning.find_one({'type' : 'COMMIT', 'commit_hash' : commit_hash})
            snapshot = self.versioning.find_one({'type' : 'SNAPSHOT', 'snapshot_hash' : commit['snapshot_hash']})
            return find_doc_from_snapshot(snapshot, id_)
        else:
            if id_ in self.__current_working_snapshot.id2hash:
                hash = self.__current_working_snapshot.id2hash[id_]
                if hash in self.__current_working_snapshot.to_be_added:
                    return self.__current_working_snapshot.to_be_added[hash]
                else:
                    return self.collection.find_one({'base.record' : hash})
            else:
                return None
        
    def find_by_hash(self, document_hash, snapshot_id=None, commit_hash=None):
        def find_doc_from_hash(snapshot, hash):
            if hash not in snapshot['document_hashes']:
                    return None
            else:
                return self.collection.find_one({'base.record' : hash})

        if snapshot_id and commit_hash or snapshot_id:
            if snapshot_id == self.current_snapshot:
                return self.find_by_hash(document_hash)
            else:
                snapshot = self.versioning.find_one({'type' : 'SNAPSHOT', '_id' : ObjectId(snapshot_id)})
                return find_doc_from_hash(snapshot, commit_hash)
        elif commit_hash:
            commit = self.versioning.find_one({'type' : 'COMMIT', 'commit_hash' : commit_hash})
            snapshot = self.versioning.find_one({'type' : 'SNAPSHOT', 'snapshot_hash' : commit['snapshot_hash']})
            return find_doc_from_hash(snapshot, commit_hash)
        else:
            if document_hash in self.__current_working_snapshot.hash2id:
                if document_hash in self.__current_working_snapshot.to_be_added:
                    return self.__current_working_snapshot[document_hash]
                else:
                    return self.collection.find_one({'base.record' : hash})
            else:
                return None

    def _DANGEROUS__delete_by_hash(self, hash_) -> None:
        self.collection.delete_one({'base.record': hash_})
    
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
        if snapshot_id and commit_hash or commit_hash:
            commit = self.versioning.find_one({'type' : 'COMMIT', 'commit_hash' : commit_hash})
            snapshot_hash = commit['snapshot_hash']
            snapshot = self.versioning.find_one({'type': 'SNAPSHOT', 'snapshot_hash' : snapshot_hash})
            if self.__current_working_snapshot:
                self.__current_working_snapshot = None
            self.__current_working_snapshot = _WorkingSnapshot(str(snapshot['_id']), snapshot['document_hashes'], snapshot_hash)
        elif snapshot_id:
            snapshot = self.versioning.find_one({'type': 'SNAPSHOT', '_id' : ObjectId(snapshot_id)})
            if self.__current_working_snapshot:
                self.__current_working_snapshot = None
            self.__current_working_snapshot = _WorkingSnapshot(str(snapshot['_id']), snapshot['document_hashes'], snapshot_hash)

    def get_commit(self, snapshot_id):
        snapshot = self.versioning.find_one({'type' : 'SNAPSHOT', '_id' : ObjectId(snapshot_id)})
        if not snapshot:
            raise RuntimeError("The snapshot does not exist")
        if len(snapshot['commit_hashes']) == 0:
            raise RuntimeError("The snapshot does not have any associated commits")
    
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

