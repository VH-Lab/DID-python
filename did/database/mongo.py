from datetime import datetime as dt
from abc import abstractmethod, ABC, abstractclassmethod
from ..globals import get_mongo_connection
from ..versioning import hash_commit, hash_document, hash_snapshot
from ..exception import NoTransactionError, NoWorkingSnapshotError, SnapshotIntegrityError
from .did_driver import DID_Driver
from ..document import DIDDocument
from ..query import Query
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

    def add_id(self, id):
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

    def insert(self, collection: Collection, session=None):
        filter = self._for_insertion()
        if not self.find_one(collection):
            if session:
                session.action_on(collection, Collection.insert_one, [filter], Collection.delete_one, [filter])
                result = session.already_executed[-1]['return_value']
            else:
                result = collection.insert_one(filter)
            return result.inserted_id
        return None

    def delete(self, collection: Collection, session=None):
        if self.find_one(collection):
            if session:
                session.action_on(collection, Collection.delete_one, [self._to_filter()],
                                  Collection.insert_one, [self._to_filter()])
            else:
                collection.delete_one(self._to_filter())

    def update(self, collection: Collection, update, session=None):
        update_filter = update._to_filter() if update else None
        if '_id' in update_filter:
            update_filter.pop('_id')
        before = self.find_one(collection)
        if before:
            if update_filter:
                if session:
                    session.action_on(collection, Collection.update_one, [self._to_filter(), {'$set': update_filter}],
                                        Collection.update_one, [update_filter, {'$set' : before}])
                else:
                    collection.update_one(self._to_filter(), {'$set': update_filter})
        else:
            if session:
                session.action_on(collection, Collection.insert_one, [update], Collection.delete, [update])
            else:
                session.insert_one(update)

class Document(MongoSchema):
    def __init__(self, document: DIDDocument = None, id=None, snapshot=None):
        super().__init__(id)
        self.data = document.data if document else None
        self.document_hash = self.data['base']['records'][0] if document else None
        self.snapshot = self.data['base']['snapshots'] if document else None
        self.document_id = document.id if document else None
        self.clause = None

    @classmethod
    def from_did_query(cls, query):
        return cls()

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

    def add_or_clause(self, document):
        self.clause = ('or', document)

    def add_and_clause(self, document):
        self.clause = ('and', document)

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
    def __init__(self, id=None, snapshot_id=None, commit_hash=None, parent_commit_hash=None, timestamp=None,
                 message=None):
        super().__init__(id)
        self.type = 'COMMIT'
        self.timestamp = dt.now().isoformat() if not timestamp else timestamp
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


class _TransactionHandler:
    class _ReturnedValue:
        def __init__(self, index):
            self.index = index

    def __init__(self, parent=None, nothrow=False):
        self.pending = []
        self.already_executed = []
        self.parent = parent
        self.commit = True
        self.has_closed = False
        self.nothrow=nothrow if not parent else True

    def __enter__(self):
        return self

    def action_on(self,
                  instance,
                  callback,
                  args_for_callback,
                  reverse_callback_success,
                  args_for_reverse_callback_success,
                  reverse_callback_fail=None,
                  args_for_reverse_callback_fail=None,
                  immediately_executed=True):
        action = {'instance': instance,
                  'callback': (callback, args_for_callback),
                  'reverse_callback_success': (reverse_callback_success, args_for_reverse_callback_success),
                  'reverse_callback_fail': (reverse_callback_fail, args_for_reverse_callback_fail),
                  'success': False,
                  'return_value': None}
        if not self.has_closed:
            if immediately_executed:
                try:
                    self._execute_single_action(action)
                except:
                    if not self.nothrow:
                        raise RuntimeError(
                            "Exception occured: type: {}, value: {}, traceback: {}".format(exc_type, exc_value, traceback))
                    else:
                        self.revert()
                        self.has_closed=True
            else:
                self.pending.append(action)

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None and exc_value is None and traceback is None:
            self._cleanup()
        else:
            self.revert()
            if not self.nothrow:
                raise RuntimeError(
                    "Exception occured: type: {}, value: {}, traceback: {}".format(exc_type, exc_value, traceback))

    def _cleanup(self):
        if not self.has_closed:
            self.has_closed = True
            try:
                if self.commit:
                    for action in self.pending:
                        self._execute_single_action(action)
                    if self.parent:
                        self.parent.already_executed.extend(self.already_executed)
                else:
                    self.revert()
            except Exception as e:
                self.revert()
                if not self.nothrow:
                    raise RuntimeError(
                        "Encountering error while executing one of the callbacks, actions unrolled, error :{}".format(
                            str(e)))

    def _execute_single_action(self, action):
        self.already_executed.append(action)
        instance = action['instance'] if 'instance' in action else None
        callback, args = action['callback']
        returned_value = self.__execute(args, instance, callback)
        action['success'] = True
        action['return_value'] = returned_value

    def revert(self):
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

    def query_return_value(self, index, immediate=False):
        if immediate:
            return self.already_executed[index]['return_value']
        return _TransactionHandler._ReturnedValue(index)

    def __execute(self, args, instance, callback):
        if args:
            for i in range(len(args)):
                if isinstance(args[i], _TransactionHandler._ReturnedValue):
                    args[i] = self.already_executed[args[i].index]['return_value']
        if isinstance(callback, _TransactionHandler._ReturnedValue):
            callback = self.already_executed[callback.index]['return_value']
        if isinstance(instance, _TransactionHandler._ReturnedValue):
            instance = self.already_executed[instance.index]['return_value']
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
        self.__current_transaction = None

    def __create_snapshot(self):
        if not self.__current_session:
            self.__current_session = _TransactionHandler()
        try:
            return Snapshot.get_head(self.versioning)
        except:
            self.__setup_version_control()
            return self.__create_snapshot()

    def __setup_version_control(self):
        with _TransactionHandler() as session:
            snapshot = Snapshot().add_snapshot_hash()
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

    @property
    def working_snapshot_id(self):
        if self.__current_working_snapshot:
            return self.__current_working_snapshot.id
        else:
            self.__current_working_snapshot = self.__create_snapshot()
            return self.__current_working_snapshot.id

    @working_snapshot_id.setter
    def working_snapshot_id(self, value):
        self.__current_working_snapshot.add_id(value)

    def save(self):
        # Should not be called inside the with statement
        if self.__current_working_snapshot:
            self.__current_working_snapshot = True
            self.__current_working_snapshot._cleanup()
            self.__current_working_snapshot = None
        else:
            if self.options.verbose_feedback:
                print('No current transactions to save.')
            else:
                raise NoTransactionError('No current transactions to save.')

    def revert(self):
        # Should not be called inside the with statement
        if self.__current_working_snapshot:
            self.__current_working_snapshot = False
            self.__current_working_snapshot._cleanup()
            self.__current_working_snapshot = None
        else:
            if self.options.verbose_feedback:
                print('No current transactions to revert.')
            else:
                raise NoTransactionError('No current transactions to revert.')

    def transaction_handler(self):
        if self.__current_working_snapshot:
            self.__current_transaction = _TransactionHandler(parent=self.__current_session)
            return self.__current_transaction
        else:
            self.__current_working_snapshot = self.__create_snapshot()
            return self.transaction_handler()

    def add(self, document, hash_) -> None:
        if self.__current_working_snapshot and self.__current_session:
            if not 'base' in document:
                document.data['base'] = {}
            if not 'record' in document['base']:
                document.data['base']['record'] = []
            document.data['base']['records'][0] = hash_
            doc = Document(document=document, snapshot=self.working_snapshot_id)
            if self.__current_transaction:
                self.__current_transaction.action_on(doc,
                                                     Document.insert, [self.collection], Document.delete,
                                                     [self.collection])
            else:
                self.__current_session.action_on(doc,
                                                 Document.insert, [self.collection], Document.delete,
                                                 [self.collection])
        else:
            if self.options.verbose_feedback:
                print('No current transactions to revert.')
            else:
                raise NoTransactionError('No current transactions to revert.')

    def upsert(self, document, hash_):
        if self.__current_working_snapshot and self.__current_session:
            original_document = Document(document=document, snapshot=self.working_snapshot_id)
            document_to_lookfor = Document(snapshot=self.working_snapshot_id)
            document_to_lookfor.document_id = document.id
            if not 'base' in document:
                document.data['base'] = {}
            if not 'record' in document['base']:
                document.data['base']['records'] = []
            document.data['base']['records'][0] = hash_
            update_to = Document(document=document, snapshot=self.working_snapshot_id)
            if self.__current_transaction:
                self.__current_transaction.action_on(document_to_lookfor,
                                                     Document.update, [self.collection, update_to], Document.update,
                                                     [self.collection, original_document])
            else:
                self.__current_session.action_on(document_to_lookfor,
                                                 Document.update, [self.collection, update_to], Document.update,
                                                 [self.collection, original_document, True])
        else:
            if self.options.verbose_feedback:
                print('No current transactions to revert.')
            else:
                raise NoTransactionError('No current transactions to revert.')

    def find(self, query=None, snapshot_id=None, commit_hash=None, in_all_history=False):
        if in_all_history:
            doc = Document.from_did_query(query)
            return doc.find(self.collection)
        elif snapshot_id and commit_hash or snapshot_id:
            doc = Document.from_did_query(query).add_snapshot(snapshot_id)
            return doc.find(self.collection)
        elif commit_hash:
            snapshot = Snapshot(commit_hash=commit_hash).find_one(self.versioning)
            if not snapshot:
                raise AttributeError("commit_hash is not associated with any snapshot")
            doc = Document.from_did_query(query).add_snapshot(snapshot.id)
            return doc.find(self.collection)
        else:
            self.find(query=query, snapshot_id=self.working_snapshot_id)

    def find_by_id(self, id_, snapshot_id=None, commit_hash=None):
        if snapshot_id and commit_hash or snapshot_id:
            doc = Document().add_snapshot(snapshot_id)
            doc.document_id = id_
            return doc.find(self.collection)
        elif commit_hash:
            snapshot = Snapshot(commit_hash=commit_hash).find_one(self.versioning)
            if not snapshot:
                raise AttributeError("commit_hash is not associated with any snapshot")
            doc = Document().add_snapshot(snapshot_id)
            doc.document_id = id_
            return doc.find(self.collection)
        else:
            self.find(id_, snapshot_id=self.working_snapshot_id)

    def find_by_hash(self, document_hash, snapshot_id=None, commit_hash=None):
        if snapshot_id and commit_hash or snapshot_id:
            doc = Document().add_snapshot(snapshot_id)
            doc.document_hash = document_hash
            return doc.find(self.collection)
        elif commit_hash:
            snapshot = Snapshot(commit_hash=commit_hash).find_one(self.versioning)
            if not snapshot:
                raise AttributeError("commit_hash is not associated with any snapshot")
            doc = Document().add_snapshot(snapshot_id)
            doc.document_hash = document_hash
            return doc.find(self.collection)
        else:
            self.find(document_hash, snapshot_id=self.working_snapshot_id)

    def _DANGEROUS__delete_by_hash(self, hash_) -> None:
        self.collection.delete_one({'base.record': hash_})

    def get_history(self, commit_hash=None):
        if commit_hash:
            return self.versioning.find({'type': 'COMMIT'})
        else:
            return self.versioning.find({'type': 'COMMIT', 'commit_hash': commit_hash})

    @property
    def current_ref(self):
        return Commit.get_head(self.versioning).commit_hash

    @property
    def current_snapshot(self):
        return self.__current_working_snapshot.add_snapshot_hash().snapshot_hash

    def set_current_ref(self, snapshot_id=None, commit_hash=None):
        pass

    def get_commit(self, snapshot_id):
        commit = Commit()
        commit.snapshot_id = snapshot_id
        return commit.find(self.versioning)

    def remove_from_snapshot(self, document_hash):
        if not self.__current_working_snapshot:
            raise NoWorkingSnapshotError('There is no snapshot open for modification.')
        doc = Document(snapshot=self.working_snapshot_id)
        doc.document_hash = document_hash
        doc.delete(self.collection)

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

    def add_commit(self, commit_hash, snapshot_id, timestamp, parent=None, message=None):
        commit = Commit(commit_hash=commit_hash, snapshot_id=snapshot_id,
                        timestamp=timestamp, parent_commit_hash=parent, message=message)
        commit.insert(self.versioning)

    def upsert_ref(self, name, commit_hash):
        """ Creates a ref if it doesn't already exist.

        :param name: ref name/tag.
        :type name: str
        :param commit_hash: Hash of associated commit.
        :type commit_hash: str
        """
        pass
