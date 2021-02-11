from datetime import datetime as dt
from abc import abstractmethod, ABC, abstractclassmethod
from ..globals import get_mongo_connection
from ..versioning import hash_commit, hash_document, hash_snapshot
from ..exception import NoTransactionError, NoWorkingSnapshotError, SnapshotIntegrityError
from .did_driver import DID_Driver
from ..document import DIDDocument
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


# Define schema used for ORM (Object Relational Mapping)
class MongoSchema(ABC):
    def __init__(self, id):
        if id:
            self.id = id if isinstance(id, ObjectId) else ObjectId(id)
        else:
            self.id = None

    @abstractmethod
    def _to_filter(self):
        pass

    @abstractmethod
    def _from_dict(self, dict):
        pass

    @abstractmethod
    def _for_insertion(self):
        pass

    def _to_filter_with_id(self):
        filter = self._to_filter()
        if self.id:
            if not isinstance(id, type(ObjectId())):
                filter['_id'] = ObjectId(self.id)
        return filter

    def find_one(self, collection: Collection):
        filter = self._to_filter_with_id()
        result = collection.find_one(filter)
        if result:
            return self._from_dict(result)
        return None

    def add_id(self):
        if id:
            self.id = id if isinstance(id, ObjectId) else ObjectId(id)
        else:
            self.id = None
        return self

    def find(self, collection: Collection):
        filter = self._to_filter_with_id()
        results = collection.find(filter)
        output = []
        for result in results:
            output.append(self._from_dict(result))
        return output

    def insert(self, collection: Collection):
        filter = self._for_insertion()
        if not self.find_one(collection):
            result = collection.insert_one(filter)
            return result.inserted_id
        return None

    def delete(self, collection: Collection):
        if self.find_one(collection):
            collection.delete_one(self._to_filter())

    def update(self, collection: Collection, update):
        update_filter = update._to_filter()
        if '_id' in update_filter:
            update_filter.pop('_id')
        if self.find_one(collection):
            collection.update_one(self._to_filter(), {'$set': update_filter})
        else:
            collection.insert_one(update)


class Document(MongoSchema):
    def __init__(self, document: DIDDocument = None, id=None, snapshot=None):
        super().__init__(id)
        self.data = document.data if document else None
        self.document_hash = self.data['base']['records'][0] if document else None
        self.snapshot = self.data['base']['snapshots'] if document else None
        self.document_id = document.id if document else None

    def _to_filter(self):
        filter = {}
        for field in self.data:
            filter['data.{}'.format(field)] = self.data[field]
        if self.document_hash:
            filter['document_hash'] = self.document_hash
        if self.document_id:
            filter['document_id'] = self.document_id
        if self.snapshot:
            filter['snapshot'] = self.snapshot
        return filter

    def _from_dict(self, dict):
        if 'data' in dict:
            return DIDDocument(data=dict['data'])
        return None

    def _for_insertion(self):
        return {'data': self.data, 'document_hash': self.document_hash,
                'snapshot': self.snapshot, 'document_id': self.document_id}

    def update_document_hash(self):
        self.document_hash = hash_document(self.data)
        return self

    def add_snapshot(self, snapshot):
        self.snapshot.append(snapshot)
        return self


class Snapshot(MongoSchema):
    def __init__(self, id=None, documents=None, snapshot_hash=None, commit_hash=None):
        super().__init__(id)
        self.type = 'SNAPSHOT'
        self.documents = [] if documents is None else documents
        self.snapshot_hash = snapshot_hash
        self.commit_hash = [] if commit_hash is None else commit_hash

    def _to_filter(self):
        filter = {'type': 'SNAPSHOT'}
        if self.documents:
            filter['documents'] = self.documents
        if self.snapshot_hash:
            filter['snapshot_hash'] = self.snapshot_hash
        if self.commit_hash:
            filter['commit_hash'] = self.commit_hash
        return filter

    def _from_dict(self, result):
        if result:
            snapshot = Snapshot()
            snapshot.id, snapshot.documents = result['_id'], result['documents']
            snapshot.document_hash, snapshot.commit_hash = result['documents'], result['commit_hash']
            return snapshot

    def _for_insertion(self):
        return {'type': 'SNAPSHOT', 'documents': self.documents
            , 'snapshot_hash': self.snapshot_hash, 'commit_hash': self.commit_hash}

    @classmethod
    def get_head(cls, collection: Collection):
        head = collection.find_one({'type': 'HEAD'})
        if head:
            last_commit = collection.find_one({'type': 'COMMIT', 'commit_hash': head['commit_hash']})
            if last_commit:
                last_snapshot = collection.find_one({'type': 'SNAPSHOT', '_id': last_commit['snapshot_id']})
                if last_snapshot:
                    snapshot = cls()
                    snapshot.id, snapshot.documents = last_snapshot['_id'], last_snapshot['documents']
                    snapshot.documents, snapshot.commit_hash = last_snapshot['documents'], last_snapshot['commit_hash']
                    return snapshot
                else:
                    raise AttributeError("HEAD points to an invalid snapshot")
            else:
                raise AttributeError("HEAD points to an invalid commit")
        else:
            raise AttributeError("HEAD cannot be found in the database")

    def add_commit_hash(self, commit_hash):
        self.commit_hash.append(commit_hash)
        return self

    def add_document(self, id, hash):
        self.documents.append({'id': id, 'hash': hash})
        return self

    def add_snapshot_hash(self):
        documents_hashes = [document['hash'] for document in self.documents]
        self.snapshot_hash = hash_snapshot(documents_hashes)
        return self


class Commit(MongoSchema):
    def __init__(self, id=None, snapshot_id=None, commit_hash=None, parent_commit_hash=None, message=None):
        super().__init__(id)
        self.type = 'COMMIT'
        self.timestamp = dt.now().isoformat()
        self.snapshot_id = snapshot_id
        self.commit_hash = commit_hash
        self.parent_commit_hash = parent_commit_hash
        self.message = message

    def _to_filter(self):
        filter = {'type': 'COMMIT', 'timestamp': self.timestamp}
        if self.snapshot_id:
            filter['snapshot_id'] = self.snapshot_id
        if self.commit_hash:
            filter['commit_hash'] = self.commit_hash
        if self.parent_commit_hash:
            filter['parent_commit_hash'] = self.parent_commit_hash
        if self.message:
            filter['message'] = self.message
        return filter

    def _from_dict(self, result):
        return Commit(result['_id'], result['snapshot_id'], result['commit_hash'],
                      result['parent_commit_hash'], result['message'])

    def _for_insertion(self):
        return {'type': 'COMMIT', 'timestamp': self.timestamp, 'snapshot_id': self.snapshot_id,
                'commit_hash': self.commit_hash, 'parent_commit_hash': self.parent_commit_hash,
                'message': self.message}

    @classmethod
    def get_head(cls, collection: Collection):
        head = collection.find_one({'type': 'HEAD'})
        if head:
            last_commit = collection.find_one({'type': 'COMMIT', 'commit_hash': head['commit_hash']})
            if last_commit:
                return cls(last_commit['_id'], last_commit['snapshot_id'], last_commit['commit_hash']
                           , last_commit['parent_commit_hash'], last_commit['message'])
            else:
                raise AttributeError("HEAD points to an invalid commit")
        else:
            raise AttributeError("HEAD cannot be found in the collection")

    def add_commit_hash(self):
        self.commit_hash = hash_commit(hash_snapshot([]), str(self.id), self.timestamp)
        return self


class _WorkingSnapshot:
    def __init__(self, snapshot_id, documents, snapshot_hash):
        self.id2hash = {doc['id']: doc['hash'] for doc in documents}
        self.hash2id = {self.id2hash[key]: key for key in self.id2hash}
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
        document_hashes = [{'id': id, 'hash': self.id2hash[id]} for id in self.id2hash]
        return {'type': 'SNAPSHOT', 'document_hashes': document_hashes, 'snapshot_hash': snapshot_hash}


class _TransactionHandler:
    class _ReturnedValue:
        def __init__(self, index):
            self.index = index

    def __init__(self):
        self.actions = []
        self.already_executed = []

    def __enter__(self):
        return self

    def action_on(self,
                  instance,
                  callback,
                  args_for_callback,
                  reverse_callback_success,
                  args_for_reverse_callback_success,
                  reverse_callback_fail=None,
                  args_for_reverse_callback_fail=None):
        self.actions.append({'instance': instance,
                             'callback': (callback, args_for_callback),
                             'reverse_callback_success': (reverse_callback_success, args_for_reverse_callback_success),
                             'reverse_callback_fail': (reverse_callback_fail, args_for_reverse_callback_fail),
                             'success': False,
                             'return_value': None})

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None and exc_value is None and traceback is None:
            try:
                for action in self.actions:
                    self.already_executed.append(action)
                    instance = action['instance'] if 'instance' in action else None
                    callback, args = action['callback']
                    returned_value = self.__execute(args, instance, callback, )
                    action['success'] = True
                    action['return_value'] = returned_value
            except Exception as e:
                for action in reversed(self.already_executed):
                    try:
                        instance = action['instance'] if 'instance' in action else None
                        reverse_callback, args = action['reverse_callback_success'] if action['success'] else \
                            action['reverse_callback_fail']
                        if reverse_callback:
                            self.__execute(args, instance, reverse_callback)
                    except Exception as e:
                        raise RuntimeError(
                            "Fail to unroll changes for action {}, please unroll manually. Error message: {}".format(
                                action, str(e)))
                raise RuntimeError(
                    "Encountering error while executing one of the callbacks, actions unrolled, error :{}".format(
                        str(e)))
        else:
            raise RuntimeError(
                "Exception occured: type: {}, value: {}, traceback: {}".format(exc_type, exc_value, traceback))

    def query_return_value(self, index):
        return _TransactionHandler._ReturnedValue(index)

    def __execute(self, args, instance, callback):
        for i in range(len(args)):
            if isinstance(args[i], _TransactionHandler._ReturnedValue):
                args[i] = self.actions[args[i].index]['return_value']
        if isinstance(callback, _TransactionHandler._ReturnedValue):
            callback = self.actions[callback.index]['return_value']
        if isinstance(instance, _TransactionHandler._ReturnedValue):
            instance = self.actions[instance.index]['return_value']
        if instance is not None and args is not None:
            returned_value = callback(instance, *list(args))
        elif instance is not None:
            returned_value = callback(instance)
        elif args:
            returned_value = callback(*list(args))
        else:
            returned_value = callback()
        return returned_value


class Mongo(DID_Driver):
    def __init__(
            self,
            connection_string=None,
            hard_reset_on_init=False,
            verbose_feedback=True,
            debug_mode=False):

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
        self.__current_session = None

    def __create_snapshot(self):
        try:
            snapshot = Snapshot.get_head(self.versioning)
            return _WorkingSnapshot(snapshot.id, snapshot.documents, snapshot.snapshot_hash)
        except:
            return self.__setup_version_control()

    def __setup_version_control(self):
        with _TransactionHandler() as session:
            snapshot, id = Snapshot().add_snapshot_hash(), []
            session.action_on(snapshot, Snapshot.insert, [self.versioning], Snapshot.delete, [self.versioning])
            session.action_on(None, lambda snapshot_id: Commit(snapshot_id=snapshot_id,
                                                               message="Initialize database").add_commit_hash(),
                              [session.query_return_value(0)], None, None)
            session.action_on(session.query_return_value(1), Commit.insert, [self.versioning], Commit.delete,
                              [self.versioning])
            session.action_on(None, lambda commit: {'type': 'HEAD', 'commit_hash': commit.commit_hash},
                              [session.query_return_value(1)], None, None)
            session.action_on(None, lambda commit: Snapshot().add_commit_hash(commit.commit_hash),
                              [session.query_return_value(1)], None, None)
            session.action_on(self.versioning, Collection.insert_one, [session.query_return_value(3)],
                              Collection.delete_one, [{'type': 'HEAD'}])
            session.action_on(snapshot, Snapshot.update, [self.versioning, session.query_return_value(4)], None, None)
            session.action_on(id, list.append, [session.query_return_value(0)], None, None)
        return _WorkingSnapshot(id[0], snapshot.documents, snapshot.snapshot_hash)

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
            document_to_add = [{'document_properties': document_to_add[hash], 'document_hash': hash} for hash in
                               document_to_add]
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
        # TODO implement find in those three scenarios
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
                return self.collection.find_one({'base.record': hash})

        if snapshot_id and commit_hash or snapshot_id:
            if snapshot_id == self.current_snapshot:
                return self.find_by_id(id_)
            else:
                snapshot = self.versioning.find_one({'type': 'SNAPSHOT', '_id': ObjectId(snapshot_id)})
                return find_doc_from_snapshot(snapshot, id_)
        elif commit_hash:
            commit = self.versioning.find_one({'type': 'COMMIT', 'commit_hash': commit_hash})
            snapshot = self.versioning.find_one({'type': 'SNAPSHOT', 'snapshot_hash': commit['snapshot_hash']})
            return find_doc_from_snapshot(snapshot, id_)
        else:
            if id_ in self.__current_working_snapshot.id2hash:
                hash = self.__current_working_snapshot.id2hash[id_]
                if hash in self.__current_working_snapshot.to_be_added:
                    return self.__current_working_snapshot.to_be_added[hash]
                else:
                    return self.collection.find_one({'base.record': hash})
            else:
                return None

    def find_by_hash(self, document_hash, snapshot_id=None, commit_hash=None):
        def find_doc_from_hash(snapshot, hash):
            if hash not in snapshot['document_hashes']:
                return None
            else:
                return self.collection.find_one({'base.record': hash})

        if snapshot_id and commit_hash or snapshot_id:
            if snapshot_id == self.current_snapshot:
                return self.find_by_hash(document_hash)
            else:
                snapshot = self.versioning.find_one({'type': 'SNAPSHOT', '_id': ObjectId(snapshot_id)})
                return find_doc_from_hash(snapshot, commit_hash)
        elif commit_hash:
            commit = self.versioning.find_one({'type': 'COMMIT', 'commit_hash': commit_hash})
            snapshot = self.versioning.find_one({'type': 'SNAPSHOT', 'snapshot_hash': commit['snapshot_hash']})
            return find_doc_from_hash(snapshot, commit_hash)
        else:
            if document_hash in self.__current_working_snapshot.hash2id:
                if document_hash in self.__current_working_snapshot.to_be_added:
                    return self.__current_working_snapshot[document_hash]
                else:
                    return self.collection.find_one({'base.record': hash})
            else:
                return None

    def _DANGEROUS__delete_by_hash(self, hash_) -> None:
        self.collection.delete_one({'base.record': hash_})

    def get_history(self, commit_hash=None):
        if commit_hash:
            return self.versioning.find({'type': 'COMMIT'})
        else:
            return self.versioning.find({'type': 'COMMIT', 'commit_hash': commit_hash})

    @property
    def current_ref(self):
        result = self.versioning.find_one({'type': 'HEAD'})
        return result['commit_hash']

    @property
    def current_snapshot(self):
        return self.__current_working_snapshot.snapshot_hash

    def set_current_ref(self, snapshot_id=None, commit_hash=None):
        if snapshot_id and commit_hash or commit_hash:
            commit = self.versioning.find_one({'type': 'COMMIT', 'commit_hash': commit_hash})
            snapshot_hash = commit['snapshot_hash']
            snapshot = self.versioning.find_one({'type': 'SNAPSHOT', 'snapshot_hash': snapshot_hash})
            if self.__current_working_snapshot:
                self.__current_working_snapshot = None
            self.__current_working_snapshot = _WorkingSnapshot(str(snapshot['_id']), snapshot['document_hashes'],
                                                               snapshot_hash)
        elif snapshot_id:
            snapshot = self.versioning.find_one({'type': 'SNAPSHOT', '_id': ObjectId(snapshot_id)})
            if self.__current_working_snapshot:
                self.__current_working_snapshot = None
            self.__current_working_snapshot = _WorkingSnapshot(str(snapshot['_id']), snapshot['document_hashes'],
                                                               snapshot_hash)

    def get_commit(self, snapshot_id):
        snapshot = self.versioning.find_one({'type': 'SNAPSHOT', '_id': ObjectId(snapshot_id)})
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
