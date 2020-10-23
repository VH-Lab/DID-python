from __future__ import annotations
from .did_database import DID_Database
from pathlib import Path
from ..document import DIDDocument
from ..query import CompositeQuery, AndQuery, OrQuery
from .utils import with_update_warning, with_delete_warning
import did.types as T
import re
import numpy as np


class FileSystem(DID_Database):
    """File system database API.

    .. currentmodule:: ndi.database.did_database

    Inherits from the :class:`DID_Database` abstract class.
    """

    def __init__(self, exp_dir, db_name='.ndi'):
        self.exp_dir = Path(exp_dir)
        self.db_dir = self.exp_dir / db_name
        self.collection_dir = self.db_dir / 'documents'

        # Initializing FS Database
        if self.exp_dir.exists() and self.exp_dir.is_dir():
            self.lookup_dir = LookupCollection(self.db_dir, 'document')
            self.collection_dir.mkdir(parents=True, exist_ok=True)
        else:
            raise FileNotFoundError(
                f'No such file or directory: \'{self.exp_dir}\'')

    def add(self, ndi_document):
        if not isinstance(ndi_document, Document):
            raise TypeError(f'Unexpected type {type(ndi_document)}. Expected type {Document}.')
        file_path = self.collection_dir / f'{ndi_document.id}.dat'
        ndi_document.set_ctx_database(self)
        if not file_path.exists():
            self.__verify_dependencies(ndi_document)
            self.upsert(ndi_document, force=True)
            self.__add_relationships(ndi_document)
        else:
            raise FileExistsError(f'File \'{file_path}\' already exists')

    def update(self, ndi_document, force=False):        
        file_path = self.collection_dir / f'{ndi_document.id}.dat'
        if file_path.exists():
            self.upsert(ndi_document, force=force)
        else:
            raise FileNotFoundError(f'File \'{file_path}\' does not exist')

    @with_update_warning
    def upsert(self, ndi_document, force=False):
        (self.collection_dir /
        f'{ndi_document.id}.dat').write_bytes(ndi_document.serialize())

    def delete(self, ndi_document, force=False):
        self.delete_by_id(ndi_document.id, force=force)

    def find(self, ndi_query=None):
        ndi_documents = [
            Document.from_flatbuffer(file.read_bytes())
            for file in self.collection_dir.glob('*.dat')
        ]

        if ndi_query is None:
            return [doc.with_ctx_database(self) for doc in ndi_documents]

        return [
            doc.with_ctx_database(self)
            for doc in ndi_documents
            if self.__parse_query(doc.data, ndi_query)
        ]

    def update_many(self, ndi_query=None, payload={}, force=False):
        ndi_documents = self.find(ndi_query)
        for ndi_document in ndi_documents:
            self.__update(ndi_document, payload, force=force)

    def delete_many(self, ndi_query=None, force=False):
        ndi_documents = self.find(ndi_query)
        for ndi_document in ndi_documents:
            self.delete(ndi_document, force=force)

    def find_by_id(self, id_):
        try:
            doc = Document.from_flatbuffer((self.collection_dir / f'{id_}.dat').read_bytes())
            return doc.with_ctx_database(self)
        except FileNotFoundError:
            return None

    def update_by_id(self, id_, payload={}, force=False):
        ndi_document = self.find_by_id(id_)
        self.__update(ndi_document, payload, force=force)

    def __update(self, ndi_document, payload, force):
        for key, value in payload.items():
            if key in ndi_document.data:
                ndi_document.data[key] = value
                self.update(ndi_document, force=force)

    @with_delete_warning
    def delete_by_id(self, id_, force=False):
        self.__delete_dependents(id_)
        (self.collection_dir / f'{id_}.dat').unlink()

    # Query Parsing Methods
    def __parse_query(self, data, ndi_query):
        if isinstance(ndi_query, CompositeQuery):
            return self.__composite_query(data, ndi_query)
        return self.__test_query(data, ndi_query)

    def __test_query(self, data, ndi_query):
        field, operator, value = ndi_query()
        try:
            for key in field.split('.'):
                data = data[key]
            if operator == 'exists':
                return True == value
            return self.__operations[operator](data, value)
        except KeyError:
            if operator == 'exists':
                return False == value
            return False

    def __composite_query(self, data, ndi_query):
        return self.__operations[type(ndi_query)]([self.__parse_query(data, query) for query in ndi_query])

    __operations = {
        AndQuery: all,
        OrQuery: any,
        '==': lambda data, value: data == value,
        '!=': lambda data, value: data != value,
        'contains': lambda data, value: value in data,
        'match': lambda data, value: re.match(value, data),
        '>': lambda data, value: data > value,
        '>=': lambda data, value: data >= value,
        '<': lambda data, value: data < value,
        '<=': lambda data, value: data <= value,
        'in': lambda data, value: data in value
    }

    def get_dependencies(self, ndi_document):
        for key, value in ndi_document.data['_dependencies'].items():
            ndi_document.data['_dependencies'][key] = self.find_by_id(value)

    def __verify_dependencies(self, ndi_document):
        return all([
            self.find_by_id(value)
            for value in ndi_document.data['_dependencies'].values()
        ])

    def __add_relationships(self, ndi_document):
        for value in ndi_document.data['_dependencies'].values():
            self.lookup_dir.add(ndi_document.id, value)

    def __delete_dependents(self, id_):
        for dependent in self.lookup_dir.find_dependents(id_):
            self.__delete_dependents(dependent)
            (self.collection_dir / f'{dependent}.dat').unlink()
            self.lookup_dir.remove(dependent, id_)

class LookupCollection:
    """Collection class for File System lookups
    """

    def __init__(self, db_dir: Path, name: str) -> None:
        """Initializes a lookup collection

        :param db_dir: [description]
        :type db_dir: Path
        :param name: Name of the lookup collection. Will be appended with "_lookup"
        :type name: str
        """
        self.collection_dir = Path(db_dir) / f'{name}_lookup'

        # Initializing Collection
        self.collection_dir.mkdir(parents=True, exist_ok=True)

    def add(self, ndi_document_id: T.NdiId, dependency_id: T.NdiId) -> None:
        """Add a document relation to the collection 

        :param ndi_document_id: [description]
        :type ndi_document_id: T.NdiId
        :param dependency_id: [description]
        :type dependency_id: T.NdiId
        """
        (self.collection_dir /
         f'{ndi_document_id}:{dependency_id}').touch(exist_ok=False)

    def remove(self, ndi_document_id: T.NdiId, dependency_id: T.NdiId) -> None:
        """Remove a document relation from the collection

        :param ndi_document_id: [description]
        :type ndi_document_id: T.NdiId
        :param dependency_id: [description]
        :type dependency_id: T.NdiId
        """
        (self.collection_dir / f'{ndi_document_id}:{dependency_id}').unlink()

    def find_dependencies(self, dependent_id: T.NdiId) -> T.List[str]:
        """Find dependencies given a dependent's id

        :param dependent_id: [description]
        :type dependent_id: T.NdiId
        :return: [description]
        :rtype: T.List[str]
        """
        return self.__find(dependent_id, 0)

    def find_dependents(self, dependency_id: T.NdiId) -> T.List[str]:
        """Find dependents given a dependency's id

        :param dependency_id: [description]
        :type dependency_id: T.NdiId
        :return: [description]
        :rtype: T.List[str]
        """
        return self.__find(dependency_id, 1)

    def __find(self, id_: T.NdiId, index: int) -> T.List[str]:
        ids = []
        for row in self.collection_dir.iterdir():
            split = row.stem.split(':')
            if id_ == split[index]:
                ids.append(split[1 - index])
        return ids


class BinaryCollection:
    def __init__(self, db_dir, name):
        self.collection_dir = Path(db_dir) / f'{name}_binary'

        # Initializing Collection
        self.collection_dir.mkdir(parents=True, exist_ok=True)

    def open_write_stream(self, document_id):
        return open(self.collection_dir / f'{document_id}.bin', 'wb')

    def open_read_stream(self, document_id):
        return open(self.collection_dir / f'{document_id}.bin', 'rb')
