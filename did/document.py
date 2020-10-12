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
        self.id = id_
        self.data = {
            # from https://github.com/VH-Lab/DID-matlab/wiki/Discussion-on-DID-document-core
            # filled with placeholders for now
            'base': {
                'id': 'id',
                'session_id': 'session_id',
                'name': 'name',
                # etc
            },
            'class': {
                'definition': 'definition',
                'validation': 'validation',
                'class_name': 'class_name',
                'superclasses': [{
                    'definition': 'definition',
                    'version': 'version',
                }],
                # etc
            },
            'depends_on': [],
            **data
        }
        # TODO: add the following to the document flatbuffer schema
        self.sub_documents = []
        self.__binary_files = []

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

