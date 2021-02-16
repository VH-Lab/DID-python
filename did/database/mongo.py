from datetime import datetime as dt
from abc import abstractmethod, ABC, abstractclassmethod
from ..globals import get_mongo_connection
from ..versioning import hash_commit, hash_document, hash_snapshot
from ..exception import NoTransactionError, NoWorkingSnapshotError, SnapshotIntegrityError
from .did_driver import DID_Driver
from ..document import DIDDocument
from ..query import Query, CompositeQuery, AndQuery, OrQuery
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


# Define schema used for Object Relational Mapping
class MongoSchema(ABC):
    def __init__(self, id):
        self.id = id

    def _to_filter(self):
        filter = {}
        for field in dir(self):
            if not field.startswith('__') and not callable(getattr(self, field)):
                if isinstance(getattr(self, field), list):
                    if getattr(self, field):
                        filter[field] =  {'$all' : getattr(self, field)}
                else:
                    if getattr(self, field) is not None:
                        filter[field] =  getattr(self, field)
        return filter

    @classmethod
    def _from_dict(cls, dict):
        if '_id' in dict:
            instance = cls(id)
        else:
            instance = cls(None)
        for field in dict:
            setattr(instance, field, dict[field])
        return instance

    def _for_insertion(self):
        kv = self._to_filter()
        if '_id' in kv:
            kv.pop('_id')
        return kv

    def find_one(self, collection: Collection):
        filter = self._to_filter()
        result = collection.find_one(filter)
        if result:
            return self._from_dict(result)
        return None

    @property
    def id(self):
        return self.id
    
    @id.setter
    def id(self, id):
        if id:
            self.id = id if isinstance(id, ObjectId) else ObjectId(id)
        else:
            self.id = None

    def find(self, collection: Collection):
        filter = self._to_filter()
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
                collection.insert_one(update)

class Head(MongoSchema):
    def __init__(self):
        super().__init__(None)
        self.type = "HEAD"
    
    def add_commit_id(self, commit_id):
        self.commit_id = commit_id
        return self


class Document(MongoSchema):
    def __init__(self, id=None, document_id=None, data=None, snapshots=None, document_hash=None):
        super().__init__(id)
        self.document_id = document_id
        self.data = data
        self.snapshots = [] if snapshots is None else snapshots
        self.document_hash = document_hash

    def _to_filter(self):
        filter = super()._to_filter()
        if 'clause' in filter:
            filter.pop('clause')
        if 'did_query' in filter:
            filter.pop('did_query')
            query_filter = self.did2mongodb(self.did_query)
            if len(filter) > 0:
                filter = {'$and' : [filter, query_filter]}
            else:
                filter = query_filter

    def did2mongodb(self, did_query):
        field, operator, value = did_query.query()
        if operator == "and":
            return {'$and' : []}
        elif operator == "or":
            return {'$or' : []}
        else:
            if operator == '==':
                return {'data.{}'.format(field) : value}
            elif operator == '!=':
                return {'data.{}'.format(field) : {'$ne' : value}}
            elif operator == 'contains':
                pass
            elif operator == 'match':
                pass
            elif operator == '>':
                pass
            elif operator == '>=':
                pass
            elif operator == '<':
                pass
            elif operator == '<=':
                pass
            elif operator == 'exists':
                pass
            elif operator == 'in':
                pass
            else:
                raise ValueError("Query operator {} is not supported".format(operator))
            
    def _for_insertion(self):
        document = super()._for_insertion()
        if 'clause' in document:
            document.pop('clause')

    @classmethod
    def from_did_query(cls, did_query):
        document = Document()
        document.did_query = did_query
        return document

    def update_document_hash(self):
        self.document_hash = hash_document(self.data)
        return self

    def add_snapshot(self, snapshot_id):
        self.snapshots.append(snapshot_id)
        return self


class Snapshot(MongoSchema):
    def __init__(self, id=None, snapshot_hash=None, commit_id = []):
        super().__init__(id)
        self.type = 'SNAPSHOT'
        self.snapshot_hash = snapshot_hash
        self.commit_id = commit_id
    
    @classmethod
    def make_working_snapshot(cls, version, collection):
        snapshot_id = Snapshot.get_head(version).id
        collection.update_many({'$all' : [snapshot_id]}, {'$addToSet' : {'snapshot_id' : snapshot_id}})
        
    @classmethod
    def get_head(cls, collection: Collection):
        last_commit = Commit.get_head(collection)
        if last_commit:
            last_snapshot = collection.find_one({'type': 'SNAPSHOT', '_id': last_commit.snapshot_id})
            if last_snapshot:
                return cls._from_dict(last_snapshot)
            else:
                raise AttributeError("HEAD points to an invalid snapshot")
        else:
            raise AttributeError("HEAD points to an invalid commit")

    def add_commit_id(self, commit_id):
        self.commit_id.append(commit_id)
        return self

    def update_snapshot_hash(self, collection=None):
        if collection:
            docs = Document(snapshots=[self.id]).find(collection)
            documents_hashes = [doc.document_hash for doc in docs]
            self.snapshot_hash = hash_snapshot(documents_hashes)
        else:
            self.snapshot_hash = hash_snapshot([])
        return self

    def get_documents(self, collection):
        return Document(snapshots=[self.id]).find(collection)


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

    @classmethod
    def get_head(cls, collection: Collection):
        head = collection.find_one({'type': 'HEAD'})
        if head:
            last_commit = collection.find_one({'type': 'COMMIT', 'commit_hash': head['commit_hash']})
            if last_commit:
                return cls._from_dict(last_commit)
            else:
                raise AttributeError("HEAD points to an invalid commit")
        else:
            raise AttributeError("HEAD cannot be found in the collection")

    def add_commit_hash(self, commit_hash=None):
        if not commit_hash:
            self.commit_hash = hash_commit(hash_snapshot([]), str(self.id), self.timestamp)
        else:
            self.commit_hash = commit_hash
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
                except Exception as ex:
                    if not self.nothrow:
                        raise ex
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
            debug_mode=False, 
            database = 'did',
            versioning_collection = 'version', 
            document_collection = 'did_document'):

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
        self.db = self.conn[database]
        self.collection = self.db[document_collection]
        self.versioning = self.db[versioning_collection]
        self.__current_working_snapshot = None
        self.__current_session = None
        self.__current_transaction = None

    def __create_snapshot(self):
        if not self.__current_session:
            # a session is associated with a new snapshot
            self.__current_session = _TransactionHandler()
        if Head().find_one:
            #create a new working snapshot
            self.__current_working_snapshot = Snapshot()
            snapshot_id = self.__current_working_snapshot.insert()

            #add all documentsin the previous snapshot to the current working snapshot
            self.__current_session.action_on(self.collection, Collection.update_many, 
                                    [{'$all' : {'snapshots' : [snapshot_id]}}, {'$addToSet' : {'snapshot_id' : snapshot_id}}], 
                                    Collection.update_many, 
                                    [{'$all' : {'snapshots' : [snapshot_id]}}, {'$pull' : {'snapshot_id' : snapshot_id}}], 
                                    Collection.update_many,
                                    [{'$all' : {'snapshots' : [snapshot_id]}}, {'$pull' : {'snapshot_id' : snapshot_id}}])
            
            self.__current_working_snapshot.id = snapshot_id
        else:
            # when head cannot be found, set up version control from scratch
            self.__setup_version_control()
            return self.__create_snapshot()

    def __setup_version_control(self):
        with _TransactionHandler() as session:
            # create a new snapshot with no commit_id
            snapshot = Snapshot().update_snapshot_hash()
            snapshot_id = snapshot.insert(self.versioning, session)

            #create a new commit that points to the snapshot we have just created
            commit = Commit(snapshot_id=snapshot_id, message = "Database initialized")
            commit_id = commit.insert(self.versioning, session)

            #create a head that points to the commit we have just created
            head = Head().add_commit_id(commit_id)
            head.insert(self.versioning, session)

            #update the snapshot so that it points to the commit we have just created
            snapshot.update(self.collection, Snapshot().add_commit_id(commit_id), session)

    @property
    def working_snapshot_id(self):
        if self.__current_working_snapshot:
            return self.__current_working_snapshot.id
        else:
            self.__current_working_snapshot = self.__create_snapshot()
            return self.__current_working_snapshot.id

    @working_snapshot_id.setter
    def working_snapshot_id(self, value):
        self.__current_working_snapshot.id = value

    def save(self):
        if self.__current_working_snapshot:
            self._close_working_snapshot(True)
        else:
            if self.options.verbose_feedback:
                print('No current transactions to save.')
            else:
                raise NoTransactionError('No current transactions to save.')
    
    def _close_working_snapshot(self, to_save):
        self.__current_session.commit = to_save
        self.__current_session._cleanup()
        self.__current_working_snapshot = self.__current_session = self.__current_transaction = None

    def revert(self):
        if self.__current_working_snapshot:
            self._close_working_snapshot(False)
        else:
            if self.options.verbose_feedback:
                print('No current transactions to save.')
            else:
                raise NoTransactionError('No current transactions to save.')

    def transaction_handler(self):
        if self.__current_working_snapshot:
            self.__current_transaction = _TransactionHandler(parent=self.__current_session)
            return self.__current_transaction
        else:
            self.__current_working_snapshot = self.__create_snapshot()
            return self.transaction_handler()

    def add(self, document, hash_):
        doc = Document(document_id=document.id, data=document.data)\
            .add_snapshot(self.current_snapshot)\
            .update_document_hash()    
        if self.__current_working_snapshot:
            if self.__current_transaction:
                doc.insert(self.collection, self.__current_transaction)
            else:
                doc.insert(self.collection, self.__current_session)
        else:
            if self.options.verbose_feedback:
                print('No working snapshot has been open')
            else:
                raise NoTransactionError('No working snapshot has been open')

    def upsert(self, document, hash_):
        #look for document in the database that has the same id as DIDDocument passed in
        original_doc = Document(document_id=document.id)\
            .add_snapshot(self.current_snapshot)

        #update document.data and the document_hash
        update_to = Document(document_id=document.id, data=document.data)\
            .update_document_hash()    
             
        if self.__current_working_snapshot:
            if self.__current_transaction:
                original_doc.update(self.collection, update_to, self.__current_transaction)
            else:
                original_doc.update(self.collection, update_to, self.__current_session)
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
            # prioritize snapshot_id over commit_hash
            return Document.from_did_query(query)\
                        .add_snapshot(snapshot_id)\
                        .find(self.collection)
        elif commit_hash:
            # first find the commit by the commit_hash
            commit = Commit(commit_hash=commit_hash)
            # find the snapshot through the commit_id
            snapshot = Snapshot(commit_id=commit.id)\
                    .find_one(self.versioning)
            if not snapshot:
                raise AttributeError("commit_hash is not associated with any snapshot")
            return Document.from_did_query(query) \
                        .add_snapshot(snapshot.id) \
                        .find(self.collection)
        # finding all the documents in the current working snapshot
        else:
            self.find(query=query, snapshot_id=self.working_snapshot_id)

    def find_by_id(self, id_, snapshot_id=None, commit_hash=None):
        self.find(query = Query('id') == id_, 
                            snapshot_id=snapshot_id, 
                            commit_hash=commit_hash,
                            in_all_history=False)

    def find_by_hash(self, document_hash, snapshot_id=None, commit_hash=None):
        self.find(query = Query('document_hash') == document_hash, 
                            snapshot_id=snapshot_id, 
                            commit_hash=commit_hash, 
                            in_all_history=False)

    def _DANGEROUS__delete_by_hash(self, hash_) -> None:
        if self.__current_transaction:
            Document(document_hash=hash_).delete(self.collection, self.__current_transaction)
        elif self.__current_session:
            Document(document_hash=hash_).delete(self.collection, self.__current_session)
        else:
            Document(document_hash=hash_).delete(self.collection)

    def get_history(self, commit_hash=None):
        commit_id = Head().find_one(self.versioning).commit_id
        all_commits = Commit().find(self.versioning)
        index_by_id = {commit[id] : commit for commit in all_commits}
        history, curr = [], index_by_id[commit_id]
        while curr.parent:
            history.append(curr)
            curr = index_by_id[curr['parent']]
        history.append(curr)
        return history

    @property
    def current_ref(self):
        return Head().find_one(self.versioning).commit_id

    @property
    def current_snapshot(self):
        snapshot = Snapshot.get_head(self.versioning)
        return (snapshot.id, snapshot.snapshot_hash)

    def set_current_ref(self, snapshot_id=None, commit_hash=None):
        with self.transaction_handler() as session:
            if commit_hash and commit_hash or snapshot_id:
                # prioritize commit_hash over snapshot_id, so find the snapshot this commit_hash points to
                snapshot_id = Commit(commit_hash=commit_hash).find_one(self.versioning).snapshot_id
                # make a new commit with the corresponding snapshot_id
                commit_id = Commit(snapshot_id=snapshot_id,
                                    message="Switch to snapshot_id: {}".format(snapshot_id))\
                                .insert(self.versioning, session)
                #make the head points to this new commit
                Head().update(self.versioning, Head().add_commit_id(commit_id), session)
            elif snapshot_id:
                commit_id = Commit(snapshot_id=snapshot_id,
                                    message="Switch to snapshot_id: {}".format(snapshot_id))\
                        .insert(self.versioning, session)
                Head().update(self.versioning, Head().add_commit_id(commit_id), session)

    def get_commit(self, snapshot_id):
        return Commit(snapshot_id=snapshot_id).find(self.versioning)

    def remove_from_snapshot(self, document_hash):
        if not self.__current_working_snapshot:
            raise NoWorkingSnapshotError('There is no snapshot open for modification.')
        self.__current_session.action_on(self.collection, Collection.update_one, 
                                [{'$all' : {'snapshots' : [self.working_snapshot_id]}, 'document_hash' : document_hash}
                                        , {'$pull' : {'snapshot_id' : self.working_snapshot_id}}], 
                                Collection.update_one, 
                                [{'$all' : {'snapshots' : [self.working_snapshot_id]}, 'document_hash' : document_hash}
                                        , {'$addToSet' : {'snapshot_id' : self.working_snapshot_id}}])

    def get_document_hash(self, document):
        if not self.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open.')
        doc = Document(document_id=document.id, snapshots=[self.current_snapshot]).find_one(self.collection)
        if doc:
            return doc.document_hash
        else:
            if self.options.verbose_feedback:
                print("the current working snapshot does not contains any document with an id of {}".format(document.id))
            return None

    def get_working_document_hashes(self):
        if not self.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open.')
        docs = Document(snapshots=[self.current_snapshot])
        return [docs.document_hash for doc in docs]

    def sign_working_snapshot(self, snapshot_hash):
        if not self.working_snapshot_id:
            raise NoWorkingSnapshotError('There is no snapshot open.')
        self.__current_working_snapshot = self.__current_working_snapshot.to_snapshot(snapshot_hash)

    def add_commit(self, commit_hash, snapshot_id, timestamp, parent=None, message=None):
        commit = Commit(commit_hash=commit_hash, snapshot_id=snapshot_id,
                        timestamp=timestamp, parent_commit_hash=parent, message=message)
        if self.__current_transaction:
            commit.insert(self.versioning, self.__current_transaction)
        elif self.__current_session:
            commit.insert(self.versioning, self.__current_session)
        else:
            commit.insert(self.versioning)

    def upsert_ref(self, name, commit_hash):
        if not Head().find_one(self.versioning):
            commit_id = Commit().add_commit_hash(commit_hash)
            if self.__current_transaction:
                Head().add_commit_id(commit_id).insert(self.versioning, self.__current_transaction)
            elif self.__current_session:
                Head().add_commit_id(commit_id).insert(self.versioning, self.__current_session)
            else:
                Head().add_commit_id(commit_id).insert(self.versioning)
