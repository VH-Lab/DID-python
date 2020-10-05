from __future__ import annotations
import ndi.types as T
from .schema import Document as build_document
import json
from .flatbuffer_object import Flatbuffer_Object
from .context import Context



class BinaryWrapper:
    def __init__(self, binary_collection: T.BinaryCollection = None, id_: str =''):
        self.id = id_
        self.binary_collection = binary_collection
    
    def connect(self, binary_collection: T.BinaryCollection = None):
        self.binary_collection = binary_collection

    def open_write_stream(self):
        return self.binary_collection.open_write_stream(self.id)

    def open_read_stream(self):
        return self.binary_collection.open_read_stream(self.id)



class Document(Flatbuffer_Object):
    """
    A flatbuffer interface for documents.

    .. currentmodule:: ndi.ndi_object

    Inherits from the :class:`NDI_Object` abstract class.
    """

    def __init__(self, data: dict = {}, name: str = '', type_: str = '', experiment_id: str = '', id_=None):
        """Creates new ndi_document

        :param id_: [description], defaults to None
        :type id_: T.NdiId, optional
        :param data: [description], defaults to None
        :type data: dict, optional
        """
        super().__init__(id_)
        self.ctx = Context()
        self.binary = BinaryWrapper()
        self.binary.id = self.id
        self.data = {
            '_metadata': {
                'name': name,
                'type': type_,
                'experiment_id': experiment_id,
                'parent_id': '',
                'asc_path': '',
                'version_depth': 0,
                'latest_version': True,
            },
            '_dependencies': {},
            '_depends_on': [],
            **data
        }

    @property
    def current(self):
        return self.refresh()

    @property
    def metadata(self):
        return self.data['_metadata']

    @metadata.setter
    def metadata(self, new_metadata):
        self.data['_metadata'] = new_metadata

    @property
    def metadata(self):
        return self.data['_metadata']

    @metadata.setter
    def metadata(self, new_metadata):
        self.data['_metadata'] = new_metadata

    @property
    def dependencies(self):
        return self.data['_dependencies']

    @dependencies.setter
    def dependencies(self, dependencies):
        self.data['_dependencies'] = dependencies

    @property
    def depends_on(self):
        return self.data['_depends_on']
    @depends_on.setter
    def depends_on(self, depends_on):
        self.data['_depends_on'] = depends_on
    
    def set_ctx_database(self, database: T.NdiDatabase) -> None:
        self.ctx.database = database

    def with_ctx_database(self, database: T.NdiDatabase) -> T.Document:
        self.ctx.database = database
        return self

    def set_ctx(self, ctx: T.Context) -> None:
        self.ctx = ctx
        self.binary.connect(self.ctx.binary_collection)

    def with_ctx(self, ctx: T.Context) -> T.Document:
        self.set_ctx(ctx)
        return self

    def _connect_binary_fork(self, binary_collection):
        self.binary = BinaryWrapper(binary_collection, self.id)

    def save_updates(self):
        """Updates version and creates a new id for this ndi_document and saves changes in database.

        Updates id, version_depth, parent_id with previous id, and document_extension with new ndi_document id.
        Adds parent_id to asc_path.

        To be used in database implementations only.
        """
        parent_id = self.id
        self.metadata['latest_version'] = False
        self.ctx.db.update(self, force=True)
        super().__init__(None)
        metadata = self.metadata
        metadata['parent_id'] = parent_id
        metadata['asc_path'] = ',' + \
            metadata['parent_id'] + metadata['asc_path']
        metadata['version_depth'] += 1
        metadata['latest_version'] = True
        self.dependencies = {}
        self.depends_on = []

        if not self.ctx:
            """Will fire if the document is not in the database (ctx is attached any time a document is added or retrieved from the database; only user-initialized Documents should not have a ctx)."""
            # TODO: make this error message more helpful when ctx is well defined
            raise RuntimeError('This document is not attached to a database.')
        else:
            self.ctx.db.add(self)

    def refresh(self):
        self_in_db = self.ctx.db.find_by_id(self.id)
        self.data = self_in_db.data
        return self

    def get_history(self):
        """oldest to newest"""
        ids = self.metadata['asc_path'].split(',')[1:]
        results = [self.ctx.db.find_by_id(id) for id in reversed(ids)]
        return [x for x in results if x]

    def __check_dependency_key_exists(self, key: str):
        dependencies = self.dependencies
        return any([key is extant for extant in dependencies.keys()])

    def __check_dependency_id_exists(self, id_: str):
        return any([id_ is extant_id for extant_id in self.dependencies.values()])

    def add_dependency(self, ndi_document: T.Document, key: str = None):
        key = key or ndi_document.metadata['name']
        self.__verify_dependency(ndi_document, key)
        self.__link_dependency(ndi_document, key)
        self.ctx.db.add(ndi_document)
        self.ctx.db.update(self, force=True)
        ndi_document.with_ctx(self.ctx)
    
    def link_dependency(self, ndi_document: T.Document, key: str = None):
        key = key or ndi_document.metadata['name']
        self.__verify_dependency(ndi_document, key)

        self.__link_dependency(ndi_document, key)

    def __verify_dependency(self, ndi_document, key):
        if self.__check_dependency_key_exists(key):
            raise RuntimeError(
                'Dependency key is already in use (dependency keys default to the document name if not specified).')
        elif self.__check_dependency_id_exists(ndi_document.id):
            dependency_name = ndi_document.metadata['name']
            own_name = self.metadata['name']
            raise RuntimeError(
                f'Document {dependency_name} is already a dependency of document {own_name}.')

    def __link_dependency(self, ndi_document, key):
        ndi_document.depends_on.append(self.id)
        new_dependency = {key: ndi_document.id}
        self.dependencies = { 
            **self.dependencies, 
            **new_dependency 
        }
        return ndi_document

    def get_dependencies(self):
        for key, value in self.dependencies.items():
            if isinstance(value, str):
                self.dependencies[key] = self.ctx.db.find_by_id(value)
        return self.dependencies

    def _get_depends_on_objects(self):
        output = []
        for id in self.depends_on:
            doc = self.ctx.db.find_by_id(id)
            if doc:
                output.append(doc)
        return output


    def delete(self, force=False, remove_history=False,):
        if force:
            deletees = list(self.get_dependencies().values())
            if remove_history:
                deletees.extend(self.get_history())
            for ndi_document in deletees:
                ndi_document.delete(force=force, remove_history=remove_history)
            self._remove_self_from_dependencies()
            self.ctx.db.delete(self, force=force)
            self.id = None
            self.data = 'This object has been deleted.'
        else:
            raise RuntimeWarning('Are you sure you want to delete this document? This will permanently remove it and its dependencies. To delete anyway, set the force argument to True. To clear the version history of this document and related dependencies, set the remove_history argument to True.')

    def _remove_self_from_dependencies(self):
        """Removes itself as a dependency from all objects in depends_on list.

        :return: [description]
        :rtype: [type]
        """
        documents = self._get_depends_on_objects()
        for d in documents:
            d.dependencies = {
                key: value
                for key, value in d.dependencies.items()
                if value != self.id
            }
            self.ctx.db.update(d, force=True)

    @classmethod
    def from_flatbuffer(cls, flatbuffer):
        """For constructing ndi_document from a flatbuffer

        :param flatbuffer: [description]
        :type flatbuffer: bytes
        :return: [description]
        :rtype: Document
        """
        document = build_document.Document.GetRootAsDocument(flatbuffer, 0)
        return cls._reconstruct(document)

    @classmethod
    def _reconstruct(cls, document):
        """For constructing ndi_document from a flatbuffer object

        :param document: [description]
        :type document: build_document.Document
        :return: [description]
        :rtype: Document
        """
        return cls(
            id_=document.Id().decode(),
            data=json.loads(document.Data())
        )

    def _build(self, builder):
        """.. currentmodule:: ndi.ndi_object

        Called in NDI_Object.serialize() as part of flatbuffer bytearray generation from Experiment instance.

        :param builder: Builder class in flatbuffers module.
        :type builder: flatbuffers.Builder
        """
        self.data['_dependencies'] = {
            key: dep.id if isinstance(dep, Document) else dep
            for key, dep in self.dependencies.items()}

        id_ = builder.CreateString(self.id)
        data = builder.CreateString(
            json.dumps(self.data, separators=(',', ':')))

        build_document.DocumentStart(builder)
        build_document.DocumentAddId(builder, id_)
        build_document.DocumentAddData(builder, data)
        return build_document.DocumentEnd(builder)

