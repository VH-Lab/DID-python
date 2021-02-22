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
from did.time import current_time


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
    '''
    Serve as the base class for all types of Document objects (Snapshot, Commit, Head & Document)
    being stored in the MongoDB database. Its purpose is to to convert MongoDB entries directly into Python Object.
    '''

    def __init__(self, id):
        self._id = id

    def iterate_through_fileds(self, ignore=None, cond=None):
        """
        Iterate through all non-private fields (one, whose name does not start with '_') of the object and turn them
        into key-value pairs

        :param ignore: the fields that will be not be included during the iteration
        :type ignore: set

        :param cond: a function that takes a field as an argument and return a boolean value indicating if
                     the field should be included during the iteration
        :type cond: function

        :return: key-value pairs of fields and its value of the object
        :rtype: dict
        """
        filter = {}
        for field in dir(self):
            if ignore and field in ignore:
                continue
            if not field.startswith('_') and not callable(getattr(self, field)):
                if cond:
                    if cond(getattr(self, field)):
                        filter[field] = getattr(self, field)
                else:
                    filter[field] = getattr(self, field)
        return filter

    @classmethod
    def fill_object_with_fields(cls, val):
        """
        Instantiate a MongoSchema instance given a key-value pairs of the object's field and its corresponding values.
        This method is used to convert the result returned by MongoDB engine into a MongoSchema object

        :param val: the key-value pairs of the object's fields and their corresponding values
        :type val: dict

        :return: a MongoSchema instance
        :rtype: did.mongo.MongoSchema
        """
        if '_id' in val:
            instance = cls(id)
        else:
            instance = cls(None)
        for field in val:
            if field != id:
                setattr(instance, field, val[field])
        return instance

    @abstractmethod
    def _to_filter(self):
        """
        Create a MongoDB search query given the values of the object's fields

        :return: MongoDB search query
        :rtype: dict
        """
        pass

    @abstractmethod
    def _for_insertion(self):
        """
        Create a MongoDB insertion query given the values of the object's fields

        :return: MongoDB insertion query
        :rtype: dict
        """
        pass

    @abstractclassmethod
    def _from_dict(self, val):
        """
        Convert a key-vaue pairs into an object instantiated with fields that matches the key-value pairs

        :param: val: key-value pairs of fields and their corresponding values
        :type: dict

        :return: a MongoSchema instance with fields and values match with val
        :rtype: did.mongo.MongoSchema
        """
        pass

    def find_one(self, collection: Collection):
        """
        Find a single document from the MongoDB Collection passed in the collection parameter that has fields matches
        with the fields of the current object

        Example Usage:

        Find a Document whose document_hash == "256":
            >> Snapshot(document_hash = "256).find_one(mongo.collection)
                <mongo.Snapshot object at 0x110e0dfd0>

        :param: collection: a MongoDB collection
        :type: pymongo.collection.Collection

        :return: a MongoSchema instance if the document is found, otherwise None
        :rtype: did.mongo.MongoSchema | None
        """
        filter = self._to_filter()
        result = collection.find_one(filter)
        if result:
            return self._from_dict(result)
        return None

    @property
    def id(self):
        """
        Represents the _id field of the MongoDB Document object

        :return: MongoDB Document's id if exists, otherwise None
        :rtype: bson.objectid.ObjectID | None
        """
        return self._id

    @id.setter
    def id(self, id):
        if id:
            self._id = id if isinstance(id, ObjectId) else ObjectId(id)
        else:
            self._id = None

    def find(self, collection: Collection):
        """
        Find all documents from the MongoDB Collection passed in the collection parameter that has fields matches
        with the fields of the current object

        Example Usage:

        Find documents whose document_hash == "256":
            >> Snapshot(document_hash = "256).find_one(mongo.collection)
                [<mongo.Snapshot object at 0x110e0dfd0>, <mongo.Snapshot object at 0x110e0dfd1>]

        :param: collection: a MongoDB collection
        :type: pymongo.collection.Collection

        :return: a MongoSchema instance if the document is found, otherwise None
        :rtype: did.mongo.MongoSchema | None
        """
        filter = self._to_filter()
        results = collection.find(filter)
        output = []
        for result in results:
            output.append(self._from_dict(result))
        return output

    def insert(self, collection: Collection, session=None):
        """
        Insert a document into a MongoDB collection specified in the collection parameter

        Example Usage:

        Inser a document whose data = {'a': 1, 'b': 2}:
            >> Document(data={'a': 1, 'b': 2}).insert(mongo.collection)
                ObjectID('6032a5c2bb4fe10fac5ebe06')

        :param: collection: a MongoDB collection
        :type: pymongo.collection.Collection

        :param: session: a Transaction Handler object, which guarentees that if an exception has been thrown
                         during insertion, the database will get reverted back to the state before the function call
        :type: mongo._TransactionHandler

        :return: the MongoDB id of the document that has been inserted; if the document already exists in the
                 collection before insertion, the function returns None
        :rtype: bson.objectid.ObjectID | None
        """
        filter = self._for_insertion()
        if not self.find_one(collection):
            if session:
                session.action_on(collection, Collection.insert_one, [filter],
                                  Collection.delete_one, [filter])
                result = session.query_return_value(-1)
            else:
                result = collection.insert_one(filter)
            return result.inserted_id
        return None

    def delete(self, collection: Collection, session=None):
        """
        Insert a document into a MongoDB collection specified in the collection parameter

        Example Usage:

        Delete a document whose data = {'a': 1, 'b': 2}:
            >> Document(data={'a': 1, 'b': 2}).delete(mongo.collection)

        :param: collection: a MongoDB collection
        :type: pymongo.collection.Collection

        :param: session: a Transaction Handler object, which guarentees that if an exception has been thrown
                         during deletion, the database will get reverted back to the state before the function call
        :type: mongo._TransactionHandler

        :return: None
        :rtype: None
        """
        if self.find_one(collection):
            if session:
                session.action_on(collection, Collection.delete_one, [self._to_filter()],
                                  Collection.insert_one, [self._to_filter()])
            else:
                collection.delete_one(self._to_filter())

    def update(self, collection: Collection, update, session=None):
        """
        Update a document into a MongoDB collection specified in the collection parameter

        Example Usage:

        Update a document whose data = {'a': 1} to data = {'a': 1, 'b'; 2}:
            >> Document(data={'a': 1).update(self.collection, Document(data={'a': 1, 'b': 2}))

        :param: collection: a MongoDB collection
        :type: pymongo.collection.Collection

        :param: session: a Transaction Handler object, which guarentees that if an exception has been thrown
                         during deletion, the database will get reverted back to the state before the function call
        :type: mongo._TransactionHandler

        :return: None
        :rtype: None
        """
        update_filter = update.iterate_through_fileds(ignore={'id'},
                                                      cond=lambda x: x is not None) if update else None
        before = self.find_one(collection)
        before_filter = before.iterate_through_fileds(ignore={'id'},
                                                      cond=lambda x: x is not None)
        if before:
            if update_filter:
                if session:
                    session.action_on(collection, Collection.update_one,
                                      [self._to_filter(), {
                                                       '$set': update_filter}],
                                      Collection.update_one, [update_filter, {'$set': before_filter}])
                else:
                    collection.update_one(self._to_filter(), {
                                          '$set': update_filter})


class Head(MongoSchema):
    """
    Head class serves as a reference to the tip of a branch whose name specified in the name field. It keeps track
    of the commit_id it is pointing to as well as the commit_hash of that commit. If name == "CURRENT", it refers to the
    currently check-out branch
    """

    def __init__(self, id=None, commit_hash=None, name="CURRENT"):
        super().__init__(id)
        self.type = "HEAD"
        self.commit_id = None
        self.commit_hash = commit_hash
        self.name = name

    def add_commit_id(self, commit_id):
        """
        Specify the commit_id that the Head points to and return the updated instance of the Head class

        :param commit_id: the commit_id that the HEAD object is pointing to
        :type: str

        :return: an updated Head instance with the new commit_id
        :rtype: mongo.Head
        """
        self.commit_id = commit_id
        return self

    def add_commit_hash(self, commit_hash):
        """
        Specify the commit_hash that the Head points to and return the updated instance of the Head class

        :param commit_hash: the commit_id that the HEAD object is pointing to
        :type: str

        :return: an updated Head instance with the new commit_hash
        :rtype: mongo.Head
        """
        self.commit_hash = commit_hash
        return self

    def _to_filter(self):
        if self.id:
            return {'_id': self.id}
        return self.iterate_through_fileds(cond=lambda x: x is not None)

    def _for_insertion(self):
        return self.iterate_through_fileds(ignore={'id'})

    @classmethod
    def _from_dict(cls, val):
        return cls.fill_object_with_fields(val)


class Document(MongoSchema):
    """
    Document class represents a particular version of DID Documents being stored in the database, which extra information about
    the document's document_hash and all the snapshots that contains this version of the document; each collection should contains
    one document per document_hash

    :param commit_id: the commit_id that the HEAD object is pointing to
    :type: str

    :return: an updated Head instance with the new commit_id
    :rtype: mongo.Head
    """

    def __init__(self, id=None, document_id=None, data=None, snapshots=None, document_hash=None):
        super().__init__(id)
        self.document_id = document_id
        self.data = data
        self.snapshots = snapshots
        self.document_hash = document_hash
        self._did_query = None

    def _to_filter(self):
        if self._did_query:
            did_query = self._did_query
            q = self._did2mongodb(did_query)
            # setting _did_query to None in order to prevent infinite loop
            self._did_query = None
            filter = self._to_filter()
            for key in filter:
                q[key] = filter[key]
            # set _did_query field back to its original state
            self._did_query = did_query
            return q
        elif self.id:
            return {'_id': self.id}
        elif self.document_hash:
            return {'document_hash': self.document_hash}
        else:
            filter = super().iterate_through_fileds(cond=lambda x: x is not None)
            if 'snapshots' in filter and self.snapshots != []:
                filter['snapshots'] = {'$all': filter['snapshots']}
            return filter

    def _for_insertion(self):
        insertion = super().iterate_through_fileds(ignore={'id'})
        if 'snapshots' in insertion and insertion['snapshots'] is None:
            insertion['snapshots'] = []
        return insertion

    @classmethod
    def _from_dict(cls, val):
        return cls.fill_object_with_fields(val)

    def _did2mongodb(self, did_query):
        if isinstance(did_query, AndQuery):
            output = {"$and": []}
            for q in did_query:
                output['$and'].append(self._did2mongodb(q))
            return output
        elif isinstance(did_query, OrQuery):
            output = {'$or': []}
            for q in did_query:
                output['$or'].append(self._did2mongodb(q))
            return output
        else:
            field, operator, value = did_query()
            if operator == '==':
                return {'data.{}'.format(field): value}
            elif operator == '!=':
                return {'data.{}'.format(field): {'$ne': value}}
            elif operator == 'contains':
                return {'data.{}'.format(field): {'$regex': '^.*{}*$'.format(value)}}
            elif operator == 'match':
                return {'data.{}'.format(field): {'$regex': value}}
            elif operator == '>':
                return {'data.{}'.format(field): {'$gt': float(value)}}
            elif operator == '>=':
                return {'data.{}'.format(field): {'$gte': float(value)}}
            elif operator == '<':
                return {'data.{}'.format(field): {'$lt': float(value)}}
            elif operator == '<=':
                return {'data.{}'.format(field): {'$lte': float(value)}}
            elif operator == 'exists':
                return {'data.{}'.format(field): {'$exists': True}}
            elif operator == 'in':
                return {'data.{}'.format(field): {'$in': value}}
            else:
                raise ValueError(
                    "Query operator {} is not supported".format(operator))

    @classmethod
    def from_did_query(cls, did_query):
        """
        Instantiate an instance of the Document class from a did.Query object used to search through the MongoDB collection
        that matches the did_query

        :param did_query: a did_query specifies the searching criteria
        :type: str

        :return: an updated Document instance that can be used to find a document in the MongoDB collection that matches
                 the did_query passed in
        :rtype: mongo.Document
        """
        document = Document()
        document._did_query = did_query
        return document

    def update_document_hash(self, hash=None):
        """
        Update the document_hash field of the object. If hash == None, hash the document using did.versioning.hash_document function

        :param hash: the document_hash of the document object
        :type: str

        :return: an updated Document instance with the new document_hash
        :rtype: mongo.Document
        """
        if hash:
            self.document_hash = hash
        else:
            self.document_hash = hash_document(DIDDocument(data=self.data))
        return self

    def add_snapshot(self, snapshot_id):
        """
        Add a new snapshot to the document object, indicating that snapshot contains this version of the DID Document

        :param snapshot: the snapshot unique id (_id field of the Snapshot document)
        :type: str | bson.objectid.ObjectId

        :return: an updated Document instance with the new snapshot_id
        :rtype: mongo.Document
        """
        if self.snapshots is None:
            self.snapshots = []
        self.snapshots.append(snapshot_id)
        return self


class Snapshot(MongoSchema):
    """
    Snapshot represents the state of the MongoDB database at a particular point of the time. It contains a list of
    DID Document of a particular version
    """

    def __init__(self, id=None, snapshot_hash=None, commit_id=None):
        super().__init__(id)
        self.type = 'SNAPSHOT'
        self.snapshot_hash = snapshot_hash
        if isinstance(commit_id, list):
            self.commit_id = commit_id
        else:
            self.commit_id = None

    def _to_filter(self):
        if self.id:
            return {'_id': self.id}
        elif self.snapshot_hash:
            # working_snapshot may contains duplicate hash with one of the saved snapshots
            return {'type': self.type, 'snapshot_hash': self.snapshot_hash}
        else:
            filter = super().iterate_through_fileds(cond=lambda x: x is not None)
            if 'commit_id' in filter and self.commit_id != []:
                filter['commit_id'] = {'$all': filter['commit_id']}
            return filter

    def _for_insertion(self):
        insertion = super().iterate_through_fileds(ignore={'id'})
        if 'commit_id' in insertion and insertion['commit_id'] is None:
            insertion['commit_id'] = []
        return insertion

    @classmethod
    def _from_dict(cls, val):
        return cls.fill_object_with_fields(val)

    @classmethod
    def make_working_snapshot(cls, version, collection):
        snapshot_id = Snapshot.get_head(version).id
        collection.update_many({'$all': [snapshot_id]}, {
                               '$addToSet': {'snapshot_id': snapshot_id}})

    @classmethod
    def get_head(cls, collection: Collection):
        """
        Create a snapshot object that represents the snapshot that the commit, which the HEAD poitns to, is pointing at

        :param collection: MongoDB collection, from where the snapshot will be searched for
        :type collection: pymongo.collection.Collection

        :return: a Snapshot object that has the same fields and values as the Snapshot document in the MongoDB collection
        :rtype: mongo.Snapshot
        """
        last_commit = Commit.get_head(collection)
        if last_commit:
            last_snapshot = collection.find_one(
                {'type': 'SNAPSHOT', '_id': last_commit.snapshot_id})
            if last_snapshot:
                return cls._from_dict(last_snapshot)
            else:
                raise AttributeError("HEAD points to an invalid snapshot")
        else:
            raise AttributeError("HEAD points to an invalid commit")

    def add_commit_id(self, commit_id):
        if self.commit_id is None:
            self.commit_id = []
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
    """
    Commit class represents a reference to a particular snapshot. Note that each snapshot may be pointed by multiple commit, but
    each commit can only poitns to one snapshot.
    """

    def __init__(self, id=None, snapshot_id=None, commit_hash=None, parent_commit_hash=None, timestamp=None,
                 message=None):
        super().__init__(id)
        self.type = 'COMMIT'
        self.timestamp = timestamp
        self.snapshot_id = snapshot_id
        self.commit_hash = commit_hash
        self.parent_commit_hash = parent_commit_hash
        self.message = message

    def _to_filter(self):
        if self.id:
            return {'_id': self.id}
        elif self.commit_hash:
            return {'commit_hash': self.commit_hash}
        else:
            return super().iterate_through_fileds(cond=lambda x: x is not None)

    def _for_insertion(self):
        insertion = super().iterate_through_fileds(ignore={'id'})
        insertion['timestamp'] = dt.now().isoformat()
        return insertion

    @classmethod
    def _from_dict(cls, val):
        return cls.fill_object_with_fields(val)

    @classmethod
    def get_head(cls, collection: Collection):
        """
        Create a Commit object that represents the the commit, which the HEAD is pointing at

        :return: a Commit object that has the same fields and values as the Snapshot document in the MongoDB collection
        :rtype: mongo.Commit
        """
        head = collection.find_one({'type': 'HEAD'})
        if head:
            last_commit = collection.find_one(
                {'type': 'COMMIT', '_id': head['commit_id']})
            if last_commit:
                return cls._from_dict(last_commit)
            else:
                raise AttributeError("HEAD points to an invalid commit")
        else:
            raise AttributeError("HEAD cannot be found in the collection")

    def add_commit_hash(self, commit_hash=None):
        if not commit_hash:
            self.commit_hash = hash_commit(hash_snapshot(
                []), str(self.snapshot_id), str(self.timestamp))
        else:
            self.commit_hash = commit_hash
        return self


class _TransactionHandler:
    """
    A class that used to keep track of all modification made to the database and is capable of reverting back
    the database back to a particular time if the user whishes to abort the changes they have created. Not meant
    be directly instantiated by user of the DID software.
    """

    def __init__(self, parent=None):
        self.already_executed = []
        self.parent = parent
        self.commit = True
        self.has_closed = False

    def __enter__(self):
        return self

    def action_on(self, instance, callback, args_for_callback, reverse_callback_success,
                  args_for_reverse_callback_success, reverse_callback_fail=None,
                  args_for_reverse_callback_fail=None):
        """
        Insert a function call into the TransactionHandler object

        :param instance: the instance that the callback function will be invoked upon
        :type instance: Object | None

        :param callback: the instance method of the instance passed in the function that we want to call
        :type callback: Function | None

        :args_for_callback: argument passed into to the 'callback' function call (e.g. instance.callback(*args_for_callback))
        :type callback: List | None

        :param reverse_callback_success: the instance method of the instance passed in the function that we want to call to revert the side effects
                                         for the callback function (e.g. instance.callback(*args_for_callback))
        :type reverse_callback_success: Function | None

        :args_for_callback_success: argument passed into to the 'reverse_callback_success' function call
        :type args_for_callback_success: List | None

        :param reverse_callback_fail: the instance method of the instance passed in the function that we want to call to revert the side effects
                                         for the callback function (e.g. instance.callback(*args_for_callback)) when that callback function throws an exception
        :type reverse_callback_fail: Function | None

        :args_for_callback_fail: argument passed into to the 'reverse_callback_sfail' function call
        :type args_for_callback_fail: List | None
        """
        action = {'instance': instance,
                  'callback': (callback, args_for_callback),
                  'reverse_callback_success': (reverse_callback_success, args_for_reverse_callback_success),
                  'reverse_callback_fail': (reverse_callback_fail, args_for_reverse_callback_fail),
                  'success': False,
                  'return_value': None}
        if not self.has_closed:
            try:
                self._execute_single_action(action)
            except Exception as ex:
                raise ex

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None and exc_value is None and traceback is None:
            self._cleanup()
        else:
            self.revert()

    def _cleanup(self):
        if not self.has_closed:
            self.has_closed = True
            if not self.commit:
                self.revert()
            elif self.parent:
                self.parent.already_executed.extend(self.already_executed)

    def _execute_single_action(self, action):
        self.already_executed.append(action)
        instance = action['instance'] if 'instance' in action else None
        callback, args = action['callback']
        returned_value = self.__execute(args, instance, callback)
        action['success'] = True
        action['return_value'] = returned_value

    def revert(self):
        """
        Revert the database back to before any of the callback function has been executed by the _TransactionHandler
        """
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

    def query_return_value(self, index):
        return self.already_executed[index]['return_value']

    def __execute(self, args, instance, callback):
        if callback:
            if instance is not None and args is not None:
                returned_value = callback(instance, *list(args))
            elif instance is not None:
                returned_value = callback(instance)
            elif args:
                returned_value = callback(*list(args))
            else:
                returned_value = callback()
            return returned_value
        else:
            return None


class Mongo(DID_Driver):
    """
    MongoDB implementation of the DID_Driver. See DID_Driver for more detailed documentation
    """

    def __init__(
            self,
            connection_string=None,
            hard_reset_on_init=False,
            verbose_feedback=True,
            debug_mode=False,
            database='did',
            versioning_collection='version',
            document_collection='did_document'):
        """ Sets up a database connection and instantiates tables as necessary.

        DID_Driver children will likely have additional parameters required for specific database setup.

        :param hard_reset_on_init: If True, clears and reinstantiates the database and it's tables, defaults to False.
        :type hard_reset_on_init: bool, optional
        :param verbose_feedback: If True, driver actions like save and revert will print logs to stdout, defaults to True.
        :type verbose_feedback: bool, optional
        """

        def __make_connection(connection_string):
            try:
                client = MongoClient(connection_string)
                client.server_info()
                return client
            except ServerSelectionTimeoutError:
                raise ConnectionError(
                    "Fail the connect to the database @{}".format(connection_string))

        if connection_string is None:
            connection_string = get_mongo_connection('raw')
        self.options = MONGODBOptions(
            hard_reset_on_init, debug_mode, verbose_feedback)
        self.conn = __make_connection(connection_string)
        if self.options.hard_reset_on_init:
            self.conn.drop_database(database)
        self.db = self.conn[database]
        self.collection = self.db[document_collection]
        self.versioning = self.db[versioning_collection]
        self._current_working_snapshot = None
        self.__current_session = None
        self.__current_transaction = None

    def __create_snapshot(self):
        if not self.__current_session:
            # a session is associated with a new snapshot
            self.__current_session = _TransactionHandler()
            self.__current_session.commit = False
        if Head().find_one(self.versioning):
            last_unsaved_snapshot = Snapshot()
            last_unsaved_snapshot.type = 'WORKING_SNAPSHOT'
            last_unsaved_snapshot = last_unsaved_snapshot.find_one(
                self.versioning)
            if not last_unsaved_snapshot:
                # create a new working snapshot
                self._new_snapshot()
            else:
                self._current_working_snapshot = last_unsaved_snapshot
                self.__current_session.action_on(last_unsaved_snapshot, None, None,
                                                 Snapshot.delete,
                                                 [self.versioning])
                self.__current_session.action_on(self.collection, None, None, Collection.delete_many,
                                                 [{'snapshots': []}])
                self.__current_session.action_on(self.collection, None, None,
                                                 Collection.update_many,
                                                 [{'snapshots': {'$all': [self.working_snapshot_id]}},
                                                  {'$pull': {'snapshots': self.working_snapshot_id}}])
        else:
            # when head cannot be found, set up version control from scratch
            self.__setup_version_control()
            self._new_snapshot()

    def _new_snapshot(self):
        # create a new working snapshot
        self._current_working_snapshot = Snapshot()
        self._current_working_snapshot.type = 'WORKING_SNAPSHOT'
        last_snapshot_id = Snapshot.get_head(self.versioning).id
        snapshot_id = self._current_working_snapshot.insert(
            self.versioning, session=self.__current_session)

        # add all documentsin the previous snapshot to the current working snapshot
        self.__current_session.action_on(self.collection, Collection.update_many,
                                         [{'snapshots': {'$all': [last_snapshot_id]}},
                                          {'$addToSet': {'snapshots': snapshot_id}}],
                                         Collection.update_many,
                                         [{'snapshots': {'$all': [last_snapshot_id]}},
                                          {'$pull': {'snapshots': snapshot_id}}],
                                         Collection.update_many,
                                         [{'snapshots': {'$all': [last_snapshot_id]}},
                                          {'$pull': {'snapshots': snapshot_id}}])
        self._current_working_snapshot.id = snapshot_id

    def __setup_version_control(self):
        with _TransactionHandler() as session:
            # create a new snapshot with no commit_id
            snapshot = Snapshot().update_snapshot_hash()
            snapshot_id = snapshot.insert(self.versioning, session)

            # create a new commit that points to the snapshot we have just created
            commit = Commit(snapshot_id=snapshot_id,
                            message="Database initialized").add_commit_hash()
            commit_id = commit.insert(self.versioning, session)

            # create a head that points to the commit we have just created
            head = Head().add_commit_id(commit_id).add_commit_hash(commit.commit_hash)
            head.insert(self.versioning, session)

            to_update = Snapshot().add_commit_id(commit_id)

            # update the snapshot so that it points to the commit we have just created
            snapshot.update(self.versioning, to_update, session)

    def __check_working_snapshot_is_mutable(self):
        # commit_id and snapshot_hash already added => working snapshot no longer mutable
        is_not_mutable = self._current_working_snapshot.commit_id and \
                         self._current_working_snapshot.snapshot_hash
        if is_not_mutable:
            raise SnapshotIntegrityError(
                'Hashed snapshots are locked and cannot be modified.')

    @property
    def working_snapshot_id(self):
        """ Gets the current working snapshot_id if one exists. If not, initializes a new snapshot and returns its id."""
        if self._current_working_snapshot:
            return self._current_working_snapshot.id
        else:
            self.__create_snapshot()
            return self._current_working_snapshot.id

    @working_snapshot_id.setter
    def working_snapshot_id(self, value):
        """ Sets the current working snapshot_id to private attribute.

        :type value: int
        """
        self._current_working_snapshot.id = value

    def save(self):
        """
        If a transaction (working snapshot) is open, the contents of the transaction are committed to the database
            and the current_transaction and working_snapshot_id are cleared.
            Otherwise, a NoTransactionError is raised.

        :raises NoTransactionError: when there is no open working snapshot
        """
        if self._current_working_snapshot:
            # look for snapshot that has the same hash
            past_snapshot = Snapshot(snapshot_hash=self._current_working_snapshot.snapshot_hash) \
                .find_one(self.versioning)
            if past_snapshot:
                old_snapshot = Snapshot()
                old_snapshot.type = "WORKING_SNAPSHOT"
                old_snapshot_id = old_snapshot.find_one(self.versioning).id
                # delete the working_snapshot
                old_snapshot.delete(self.versioning, self.__current_session)
                # update the past_snapshot
                past_snapshot.update(
                    self.versioning, self._current_working_snapshot, self.__current_session)
                # delete all the current working snapshot id
                # TODO consider attach this to a session
                self.collection.update_many({"snapshots": {"$all": [self.working_snapshot_id]}},
                                            {"$pull": {"snapshots": old_snapshot_id}})
            else:
                # update snapshot
                self._current_working_snapshot.type = "SNAPSHOT"
                Snapshot(id=ObjectId(self.working_snapshot_id)).update(self.versioning, self._current_working_snapshot,
                                                                       self.__current_session)
            self._close_working_snapshot(True)
        else:
            if self.options.verbose_feedback:
                print('No current transactions to save.')
            else:
                raise NoTransactionError('No current transactions to save.')

    def _close_working_snapshot(self, to_save):
        if self.__current_transaction:
            self.__current_transaction.commit = to_save
            self.__current_transaction._cleanup()
        self.__current_session.commit = to_save
        self.__current_session._cleanup()
        self._current_working_snapshot = self.__current_session = self.__current_transaction = None

    def revert(self):
        """
        If a transaction (working snapshot) is open, it and the working_snapshot_id are cleared without being committed to the database.
        Otherwise, a NoTransactionError is raised.

        :raises NoTransactionError: when there is no open working snapshot
        """
        if self._current_working_snapshot:
            self._close_working_snapshot(False)
        else:
            if self.options.verbose_feedback:
                print('No current transactions to save.')
            else:
                raise NoTransactionError('No current transactions to save.')

    def transaction_handler(self):
        """ Context manager for transactions (working snapshots).
            Must ensure that current_transaction and working_snapshot_id are available.
            If they do not already exist, they should be instantiated.

        :rtype: mongo._TransactionHandler
        """
        if self._current_working_snapshot:
            self.__current_transaction = _TransactionHandler(
                parent=self.__current_session)
            return self.__current_transaction
        else:
            self.__create_snapshot()
            return self.transaction_handler()

    def add(self, document, hash_):
        """ Add a document and its hash to the current transaction.

        :type document: DID_Document
        :param hash_: See did/verisioning.py::hash_document.
        :type hash_: str
        """
        doc = Document(document_id=document.id, data=document.data) \
            .update_document_hash(hash_)
        if self._current_working_snapshot:
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
        """
        Add a document and its hash to the current transaction.
        If the document already exists, it and its hash should be updated.

        :type document: DID_Document
        :param hash_: See did/verisioning.py::hash_document.
        :type hash_: str
        """
        if self._current_working_snapshot:
            # look for document in the database that has the same id as DIDDocument passed in
            original_doc = Document(document_id=document.id) \
                .add_snapshot(self.working_snapshot_id)\
                .find_one(self.collection)
            if original_doc:
                # remove the current working snapshot_id from the old document_hash
                self.remove_from_snapshot(original_doc.document_hash)
            self.add(document, hash_)
        else:
            if self.options.verbose_feedback:
                print('No current transactions to revert.')
            else:
                raise NoTransactionError('No current transactions to revert.')

    def add_to_snapshot(self, document_hash):
        """
        Adds document hash to working snapshot.

        Note: should be used in context of self.transaction_handler.

        :type document_hash: str
        :raises NoWorkingSnapshotError: Thrown when current_transaction or working_snapshot_id do not exist.
        """
        if self._current_working_snapshot:
            # look for document by the document_hash
            doc = Document(document_hash=document_hash).find_one(
                self.collection)
            if doc:
                new_snapshots = doc.add_snapshot(
                    self.working_snapshot_id).snapshots
                new_doc = Document(snapshots=new_snapshots)
                if self.__current_transaction:
                    doc.update(self.collection, new_doc,
                               self.__current_transaction)
                else:
                    doc.update(self.collection, new_doc,
                               self.__current_session)
        else:
            if self.options.verbose_feedback:
                print('No current transactions to revert.')
            else:
                raise NoTransactionError('No current transactions to revert.')

    def find(self, query=None, snapshot_id=None, commit_hash=None, in_all_history=False):
        """
         Find all documents matching given parameters.
            If snapshot_id, commit_hash, and in_all_history are left as default,
            then finds the matching documents as they exists in the current transaction.
            Given all three, in_all_history > snapshot_id > commit_hash.

        :param query: Filters for documents that match the given query. If None, no filter is applied.
          See did/query.py::Query. defaults to None
        :type query: Query, optional
        :param snapshot_id: Filters for documents that were part of the snapshot with the given ID. defaults to None
        :type snapshot_id: int, optional
        :param commit_hash: Filters for documents that are part of the commit with the given hash. defaults to None
        :type commit_hash: string, optional
        :param in_all_history: If True, applies no version filter
          (multiple versions of the same document or deleted documents may be returned). defaults to False
        :type in_all_history: bool, optional
        :return: A list of document matching the query parameters.
        :rtype: T.List
        """
        if snapshot_id and not isinstance(snapshot_id, ObjectId):
            snapshot_id = ObjectId(snapshot_id)
        if in_all_history:
            docs = Document.from_did_query(query).find(self.collection)
            return [DIDDocument(doc.data) for doc in docs]
        elif (snapshot_id and commit_hash) or snapshot_id:
            # prioritize snapshot_id over commit_hash
            docs = Document.from_did_query(query) \
                .add_snapshot(snapshot_id) \
                .find(self.collection)
            return [DIDDocument(doc.data) for doc in docs]
        elif commit_hash:
            # first find the commit by the commit_hash
            commit = Commit(commit_hash=commit_hash).find_one(self.versioning)
            if not commit:
                raise AttributeError("commit_hash is not associated with any snapshot")
            # find the snapshot through the commit_id
            snapshot = Snapshot(commit_id=[commit.id]) \
                .find_one(self.versioning)
            if not snapshot:
                raise AttributeError("commit_hash is not associated with any snapshot")
            docs = Document.from_did_query(query) \
                .add_snapshot(snapshot.id) \
                .find(self.collection)
            return [DIDDocument(doc.data) for doc in docs]
        # finding all the documents in the current working snapshot
        else:
            return self.find(query=query, snapshot_id=self.working_snapshot_id)

    def find_by_id(self, id_, snapshot_id=None, commit_hash=None, in_all_history=False):
        """ 
        Find the document with the given id. 
            If snapshot_id and commit_hash are left as default, then finds the document as it exists in the current transaction.
            Given both, snapshot_id > commit_hash.

        :param query: Filters for documents that match the given query. If None, no filter is applied.
          See did/query.py::Query. defaults to None
        :type query: Query, optional
        :param snapshot_id: Filters for documents that were part of the snapshot with the given ID. defaults to None
        :type snapshot_id: int, optional
        :param commit_hash: Filters for documents that are part of the commit with the given hash. defaults to None
        :type commit_hash: string, optional
        :return: The document matching the query parameters or None.
        :rtype: T.List | None
        """
        if snapshot_id and not isinstance(snapshot_id, ObjectId):
            snapshot_id = ObjectId(snapshot_id)
        if in_all_history:
            docs = Document(document_id=id_)
            return [DIDDocument(data=doc.data) for doc in docs]
        if (snapshot_id and commit_hash) or snapshot_id:
            docs = Document(document_id=id_).add_snapshot(snapshot_id).find(self.collection)
            return [DIDDocument(data=doc.data) for doc in docs]
        elif commit_hash:
            commit = Commit(commit_hash=commit_hash).find_one(self.versioning)
            if not commit:
                raise AttributeError("commit_hash is not associated with any snapshot")
            return self.find_by_id(id_=id_, snapshot_id=commit.snapshot_id)
        else:
            return self.find_by_id(id_=id_, snapshot_id=self.working_snapshot_id)

    def find_by_hash(self, document_hash, snapshot_id=None, commit_hash=None, in_all_history=False):
        """ 
        Find the document with the given hash. 
            If snapshot_id and commit_hash are left as default, then finds the document if it exists in the current transaction.
            Given both, snapshot_id > commit_hash.

        :param query: Filters for documents that match the given query. If None, no filter is applied.
          See did/query.py::Query. defaults to None
        :type query: Query, optional
        :param snapshot_id: Filters for documents that were part of the snapshot with the given ID. defaults to None
        :type snapshot_id: int, optional
        :param commit_hash: Filters for documents that are part of the commit with the given hash. defaults to None
        :type commit_hash: string, optional
        :return: The document matching the query parameters or None.
        :rtype: T.List | None
        """
        if snapshot_id and not isinstance(snapshot_id, ObjectId):
            snapshot_id = ObjectId(snapshot_id)
        doc = Document(document_hash=document_hash).find_one(self.collection)
        if doc:
            if in_all_history:
                return DIDDocument(data=doc.data)
            if (snapshot_id and commit_hash) or snapshot_id:
                return DIDDocument(data=doc.data) if snapshot_id in doc.snapshots else None 
            elif commit_hash:
                commit = Commit(commit_hash=commit_hash).find_one(self.versioning)
                if not commit:
                    raise AttributeError("commit_hash is not associated with any snapshot")
                snapshot_id = commit.snapshot_id
                return DIDDocument(data=doc.data) if snapshot_id in doc.snapshots else None
            else:
                return self.find_by_hash(document_hash=document_hash, snapshot_id=self.working_snapshot_id)
        else:
            return None

    def _DANGEROUS__delete_by_hash(self, hash_) -> None:
        """ Deletes the document with the given hash (hashes are unique).
            For use when removing documents from current transaction.

        WARNING: This method modifies the database without version support. Usage of this method may break your database history.

        :param hash_: See did/verisioning.py::hash_document.
        :type hash_: string
        """

        if self.__current_transaction:
            Document(document_hash=hash_).delete(self.collection, self.__current_transaction)
        elif self.__current_session:
            Document(document_hash=hash_).delete(self.collection, self.__current_session)
        else:
            Document(document_hash=hash_).delete(self.collection)

    def get_history(self, commit_hash=None):
        """ Returns history from given commit, with each commit including 
            the snapshot_id, commit_hash, timestamp, ref_names:List[str], and depth.
            Ordered from recent first.
            commit_hash defaults to current commit.

        :param commit_hash: See did/verisioning.py::hash_commit.
        :type commit_hash: string
        """

        commit_hash = Head().find_one(self.versioning).commit_hash
        all_commits = Commit().find(self.versioning)
        index_by_hash = {commit.commit_hash: commit for commit in all_commits}
        history, curr = [], index_by_hash[commit_hash]
        while curr.parent_commit_hash:
            history.append(curr.iterate_through_fileds())
            curr = index_by_hash[curr.parent_commit_hash]
        history.append(curr.iterate_through_fileds())
        return history

    @property
    def current_ref(self):
        """ Returns the commit hash of the CURRENT ref."""
        return Head().find_one(self.versioning)

    @property
    def current_snapshot(self):
        """ Returns the snapshot_id and hash associated with CURRENT ref.

        Note: This is not necessarily equivalent to working snapshot. The CURRENT ref points to a commit,
              which is equivalent to a saved snapshot. The working snapshot is by definition not yet saved.
        """
        snapshot = Snapshot.get_head(self.versioning)
        return snapshot.snapshot_hash

    def set_current_ref(self, snapshot_id=None, commit_hash=None):
        """ Sets the CURRENT ref to the given snapshot or commit.
            Given both, commit_hash > snapshot_id.

        :param snapshot_id: defaults to None
        :type snapshot_id: int, optional
        :param commit_hash: defaults to None
        :type commit_hash: str, optional
        :raises RuntimeWarning: [description]
        """
        if self._current_working_snapshot:
            raise SnapshotIntegrityError("Before proceeding, either save or revert your current working snapshot")
        if snapshot_id and not isinstance(snapshot_id, ObjectId):
            snapshot_id = ObjectId(snapshot_id)
        if commit_hash or (snapshot_id and commit_hash):
            # prioritize commit_hash over snapshot_id, so find the snapshot this commit_hash points to
            commit = Commit(commit_hash=commit_hash).find_one(self.versioning)
            if commit:
                snapshot_id = commit.snapshot_id
            else:
                raise RuntimeError("Attempt to set current ref to an invalid snapshot; operation aborted")
            self._switch_to_snapshot(snapshot_id)
        elif snapshot_id:
            self._switch_to_snapshot(snapshot_id)

    def _switch_to_snapshot(self, snapshot_id):
        past_snapshot = Snapshot(id=snapshot_id).find_one(self.versioning)
        if not past_snapshot:
            raise RuntimeError("Attempt to set current ref to an invalid snapshot; operation aborted")
        head_commit = Commit.get_head(self.versioning)
        new_commit_hash = hash_commit(past_snapshot.snapshot_hash, str(past_snapshot.id),
                                    current_time(), head_commit.commit_hash)
        # make a new commit with the corresponding snapshot_id
        commit = Commit(commit_hash=new_commit_hash, snapshot_id=snapshot_id,
                                    timestamp=current_time(), parent_commit_hash=head_commit.commit_hash, 
                                    message="Switch to snapshot_id: {}".format(snapshot_id))
        commit_id = commit.insert(self.versioning, self._which_session())
        #update the past_snapshot
        past_snapshot.add_commit_id(commit_id)
        past_snapshot.update(self.versioning, past_snapshot, session=self._which_session())        
        # make the head points to this new commit
        ref = Head(name="CURRENT").find_one(self.versioning)
        ref.update(self.versioning, ref.add_commit_id(commit_id) \
            .add_commit_hash(new_commit_hash), self._which_session())

    def get_commit(self, snapshot_id):
        """ Gets the commit hash associated with the given snapshot.

        :param snapshot_id: A snapshot number
        :type snapshot_id: int
        :raises RuntimeError: Thrown when snapshot_id does not have associated commits.
        :return: commit_hash
        :rtype: str
        """

        if not isinstance(snapshot_id, ObjectId):
            snapshot_id = ObjectId(snapshot_id)
        return Commit(snapshot_id=snapshot_id).find(self.versioning)

    def remove_from_snapshot(self, document_hash):
        """ Removes document hash from working snapshot.

        Note: should be used in context of self.transaction_handler.

        :type document_hash: str
        :raises NoWorkingSnapshotError: Thrown when current_transaction or working_snapshot_id do not exist.
        """

        if not self._current_working_snapshot:
            raise NoWorkingSnapshotError('There is no snapshot open for modification.')
        doc = Document(document_hash=document_hash).find_one(self.collection)
        if doc:
            self.__current_session.action_on(self.collection, Collection.update_one,
                                             [{'snapshots': {'$all': [self.working_snapshot_id]},
                                               'document_hash': document_hash}
                                                 , {'$pull': {'snapshots': self.working_snapshot_id}}],
                                             Collection.update_one,
                                             [{'snapshots': {'$all': [self.working_snapshot_id]},
                                               'document_hash': document_hash}
                                                 , {'$addToSet': {'snapshots': self.working_snapshot_id}}])
            doc_to_be_deleted = Document(snapshots=[]).find_one(self.collection)
            if doc_to_be_deleted:
                doc_to_be_deleted.delete(self.collection, self.__current_session)

    def get_document_hash(self, document):
        """ Gets the documents hash in the working snapshot.

        :type document: DID_Document
        :rtype: str | None
        """
        if document:
            if not self._current_working_snapshot:
                raise NoWorkingSnapshotError('There is no snapshot open.')
            document = document[0] if isinstance(document, list) else document
            doc = Document(document_id=document.id, snapshots=[self.working_snapshot_id]).find_one(self.collection)
            if doc:
                return doc.document_hash
            else:
                if self.options.verbose_feedback:
                    print("the current working snapshot does not contains any document with an id of {}"
                        .format(document.id))
                return None

    def get_working_document_hashes(self):
        """ Gets the hashes of all documents in the working snapshot.

        :rtype: [str]
        """
        if not self._current_working_snapshot:
            raise NoWorkingSnapshotError('There is no snapshot open.')
        docs = Document(snapshots=[self.working_snapshot_id]).find(self.collection)
        return [doc.document_hash for doc in docs]

    def sign_working_snapshot(self, snapshot_hash):
        """ Sets hash to snapshot. Once this is done, the snapshot cannot be mutated.

        :param snapshot_hash: See did.versioning::hash_snapshot.
        :type snapshot_hash: [type]
        :raises SnapshotIntegrityError: Thrown when working snapshot already has a hash.
        """
        self.__check_working_snapshot_is_mutable()
        if not self._current_working_snapshot:
            raise NoWorkingSnapshotError('There is no snapshot open.')
        past_snapshot = Snapshot(snapshot_hash=snapshot_hash).find_one(self.versioning)
        if past_snapshot:
            # snapshot already exists, replace the current working_snapshot with the past snapshot
            self._current_working_snapshot = past_snapshot
        else:
            self._current_working_snapshot.snapshot_hash = snapshot_hash

    def add_commit(self, commit_hash, snapshot_id, timestamp, parent=None, message=None):
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
        if self._current_working_snapshot:
            if isinstance(snapshot_id, str):
                snapshot_id = ObjectId(snapshot_id)
            commit = Commit(commit_hash=commit_hash, snapshot_id=snapshot_id,
                            timestamp=timestamp, parent_commit_hash=parent, message=message)
            commit_id = commit.insert(self.versioning, self._which_session())
            self._current_working_snapshot.add_commit_id(commit_id)
            Snapshot(id=ObjectId(self.working_snapshot_id)).update(self.versioning,
                                                                self._current_working_snapshot, self._which_session())
        else:
            if self.options.verbose_feedback:
                print('Cannot create a commit without an open snapshot')
            else:
                raise NoTransactionError('There is no snapshot open')
    
    def upsert_ref(self, name, commit_hash):
        """ Creates a ref if it doesn't already exist.

        :param name: ref name/tag.
        :type name: str
        :param commit_hash: Hash of associated commit.
        :type commit_hash: str
        """
        ref = Head(name=name).find_one(self.versioning)
        if not ref:
            commit_id = Commit().add_commit_hash(commit_hash).find_one(self.versioning).id
            Head().add_commit_id(commit_id) \
                .add_commit_hash(commit_hash) \
                .insert(self.versioning, self._which_session())
        else:
            # simply update the commit_id
            commit_id = Commit().add_commit_hash(commit_hash).find_one(self.versioning).id
            ref.update(self.versioning, ref.add_commit_id(commit_id) \
                       .add_commit_hash(commit_hash), self._which_session())

    def _which_session(self):
        if self.__current_transaction and self.__current_transaction.has_closed == False:
            return self.__current_transaction
        elif self.__current_session and self.__current_session.has_closed == False:
            return self.__current_session
        else:
            return None
