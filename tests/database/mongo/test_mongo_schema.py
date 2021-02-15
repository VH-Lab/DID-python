from did.database.mongo import Mongo, MongoSchema, Snapshot, Document, Commit, _TransactionHandler
from did.versioning import hash_document
from did import DIDDocument
import pytest

CONNECTION_STRING_FOR_TESTING = "mongodb://localhost:27017"
DB = "test_did"
VERSIONING_COLLECTION = "version"
DOCUMENT_COLLECTION = "documents"

MONGO_CONNECTION = Mongo(connection_string=CONNECTION_STRING_FOR_TESTING, 
                            database=DB, 
                            versioning_collection=VERSIONING_COLLECTION, 
                            document_collection=DOCUMENT_COLLECTION)
@pytest.fixture
def collection():
    return MONGO_CONNECTION.collection

@pytest.fixture
def versioning():
    return MONGO_CONNECTION.versioning

@pytest.fixture
def mock_document_data():
    return [
        {
            'base': {
                'id': '0',
                'session_id': '2387492',
                'name': 'A',
                'datestamp': '2020-10-28T08:12:20+0000',
                'snapshots': [],
                'records': [],
            },
            'depends_on': [],
            'dependencies': [],
            'binary_files': [],
            'document_class': {
                'definition': '$NDIDOCUMENTPATH/ndi_document_app.json',
                'validation': '$NDISCHEMAPATH/ndi_document_app_schema.json',
                'class_name': 'ndi_document_app',
                'property_list_name': 'app',
                'class_version': 1,
                'superclasses': [{
                    'definition': '$NDIDOCUMENTPATH/base_document.json'
                }],
            },
            'app': {
                'a': True,
                'b': True
            },
        },
        {
            'base': {
                'id': '1',
                'session_id': '2387492',
                'name': 'B',
                'datestamp': '2020-10-28T08:12:20+0000',
                'snapshots': [],
                'records': [],
            },
            'depends_on': [],
            'dependencies': [],
            'binary_files': [],
            'document_class': {
                'definition': '$NDIDOCUMENTPATH/ndi_document_app.json',
                'validation': '$NDISCHEMAPATH/ndi_document_app_schema.json',
                'class_name': 'ndi_document_app',
                'property_list_name': 'app',
                'class_version': 1,
                'superclasses': [{
                    'definition': '$NDIDOCUMENTPATH/base_document.json'
                }],
            },
            'app': {
                'a': True,
                'b': False
            },
        },
        {
            'base': {
                'id': '2',
                'session_id': '2387492',
                'name': 'C',
                'datestamp': '2020-10-28T08:12:20+0000',
                'snapshots': [],
                'records': [],
            },
            'depends_on': [],
            'dependencies': [],
            'binary_files': [],
            'document_class': {
                'definition': '$NDIDOCUMENTPATH/ndi_document_app.json',
                'validation': '$NDISCHEMAPATH/ndi_document_app_schema.json',
                'class_name': 'ndi_document_app',
                'property_list_name': 'app',
                'class_version': 1,
                'superclasses': [{
                    'definition': '$NDIDOCUMENTPATH/base_document.json'
                }],
            },
            'app': {
                'a': False,
                'b': False
            },
        },
    ]

@pytest.fixture
def documents(mock_document_data):
    documents = []
    for data in mock_document_data:
        documents.append(DIDDocument(data=data))
    return documents

def document_equals(doc1, doc2):
    return doc1.data == doc2.data


def test_normal_operation(documents, collection, versioning):
    with _TransactionHandler() as session:
        session.commit = False
        for document in documents:
            Document(document=document).insert(collection, session)
        for document in documents:
            assert document_equals(Document(document=document).find_one(collection), document)
        
        snapshot1 = Snapshot().add_document(documents[0].id, hash_document(documents[0]))\
                            .add_document(documents[1].id, hash_document(documents[1]))
        snapshot1.add_snapshot_hash()
        snapshot1.insert(versioning, session)