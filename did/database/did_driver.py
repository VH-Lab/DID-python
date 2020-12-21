from __future__ import annotations
import did.types as T
from abc import ABC, abstractmethod
from contextlib import contextmanager

from ..document import DIDDocument


class DID_Driver(ABC):
    """
    Abstract class for DID database drivers.
    Child classes of :class:`DID_Driver` are standardized, and share the same base methods and data signatures.
    """
    _collections: T.Dict = {
        DIDDocument: None
    }

    @abstractmethod
    def __init__(
        self, 
        hard_reset_on_init: bool = False,
        verbose_feedback: bool = True,
    ) -> None:
        """ Sets up a database connection and instantiates tables as necessary.

        DID_Driver children will likely have additional parameters required for specific database setup.

        :param hard_reset_on_init: If True, clears and reinstantiates the database and it's tables, defaults to False.
        :type hard_reset_on_init: bool, optional
        :param verbose_feedback: If True, driver actions like save and revert will print logs to stdout, defaults to True.
        :type verbose_feedback: bool, optional
        """
        pass

    @property
    def working_snapshot_id(self):
        """ Gets the current working snapshot_id if one exists. If not, initializes a new snapshot and returns its id."""
        pass

    @working_snapshot_id.setter
    def working_snapshot_id(self, value):
        """ Sets the current working snapshot_id to private attribute.

        :type value: int
        """
        pass

    def save(self):
        """ If a transaction (working snapshot) is open, the contents of the transaction are committed to the database
            and the current_transaction and working_snapshot_id are cleared.
            Otherwise, a NoTransactionError is raised. 

        :raises NoTransactionError: [description]
        """
        pass
    
    def revert(self):
        """If a transaction (working snapshot) is open, it and the working_snapshot_id are cleared without being committed to the database.
          Otherwise, a NoTransactionError is raised. 

        :raises NoTransactionError: [description]
        """
        pass

    @contextmanager
    def transaction_handler(self) -> T.Generator:
        """ Context manager for transactions (working snapshots).
            Must ensure that current_transaction and working_snapshot_id are available.
            If they do not already exist, they should be instantiated.

        :rtype: T.Generator
        :yield: (optional)
        :rtype: Iterator[T.Generator]
        """
        pass

    def add(self, document, hash_) -> None:
        """ Add a document and its hash to the current transaction.

        :type document: DID_Document
        :param hash_: See did/verisioning.py::hash_document.
        :type hash_: str
        """
        pass
    
    def upsert(self, document, hash_):
        """Add a document and its hash to the current transaction.
          If the document already exists, it and its hash should be updated.

        :type document: DID_Document
        :param hash_: See did/verisioning.py::hash_document.
        :type hash_: str
        """
        pass

    def find(self, query=None, snapshot_id=None, commit_hash=None, in_all_history=False) -> T.List:
        """ Find all documents matching given parameters. 
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
        pass

    def find_by_id(self, id_, snapshot_id=None, commit_hash=None):
        """ Find the document with the given id. 
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
        pass

    def find_by_hash(self, document_hash, snapshot_id=None, commit_hash=None):
        """ Find the document with the given hash. 
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
        pass

    def _DANGEROUS__delete_by_hash(self, hash_) -> None:
        """ Deletes the document with the given hash (hashes are unique).
            For use when removing documents from current transaction.

        WARNING: This method modifies the database without version support. Usage of this method may break your database history.

        :param hash_: See did/verisioning.py::hash_document.
        :type hash_: string
        """
        pass

    def get_history(self, commit_hash=None):
        """ Returns history from given commit, with each commit including 
            the snapshot_id, commit_hash, timestamp, ref_names:List[str], and depth.
            Ordered from recent first.
            commit_hash defaults to current commit.

        :param commit_hash: See did/verisioning.py::hash_commit.
        :type commit_hash: string
        """
        pass

    @property
    def current_ref(self):
        """ Returns the commit hash of the CURRENT ref."""
        pass

    @property
    def current_snapshot(self):
        """ Returns the snapshot_id and hash associated with CURRENT ref.

        Note: This is not necessarily equivalent to working snapshot. The CURRENT ref points to a commit,
              which is equivalent to a saved snapshot. The working snapshot is by definition not yet saved.
        """
        pass
    
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
        """ Adds document hash to working snapshot.

        Note: should be used in context of self.transaction_handler.

        :type document_hash: str
        :raises NoWorkingSnapshotError: Thrown when current_transaction or working_snapshot_id do not exist.
        """
        pass
    
    def remove_from_snapshot(self, document_hash):
        """ Removes document hash from working snapshot.

        Note: should be used in context of self.transaction_handler.

        :type document_hash: str
        :raises NoWorkingSnapshotError: Thrown when current_transaction or working_snapshot_id do not exist.
        """
        pass

    def get_document_hash(self, document):
        """ Gets the documents hash in the working snapshot.

        :type document: DID_Document
        :rtype: str | None
        """
        pass

    def get_working_document_hashes(self):
        """ Gets the hashes of all documents in the working snapshot.

        :rtype: [str]
        """
        pass

    def sign_working_snapshot(self, snapshot_hash):
        """ Sets hash to snapshot. Once this is done, the snapshot cannot be mutated.

        :param snapshot_hash: See did.versioning::hash_snapshot.
        :type snapshot_hash: [type]
        :raises SnapshotIntegrityError: Thrown when working snapshot already has a hash.
        """
        pass
    
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