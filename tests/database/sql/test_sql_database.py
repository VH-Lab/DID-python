from did.database.sql import SQL
from did.document import DIDDocument
import pytest
import json

mock_document_data = [
    {
        'base': {
            'id': '0',
            'session_id': '2387492',
            'name': 'A',
            'datestamp': '2020-10-28T08:12:20+0000',
            'version': '1',
        },
        'depends_on': [],
        'document_class': {
            'definition': '$NDIDOCUMENTPATH\/ndi_document_app.json',
            'validation': '$NDISCHEMAPATH\/ndi_document_app_schema.json',
            'class_name': 'ndi_document_app',
            'property_list_name': 'app',
            'class_version': 1,
            'superclasses': [{
                'definition': '$NDIDOCUMENTPATH\/base_document.json'
            }],
        },
        'app': {
            'a': 'b',
            'c': 'd',
        },
    },
    {
        'base': {
            'id': '1',
            'session_id': '2387492',
            'name': 'B',
            'datestamp': '2020-10-28T08:12:20+0000',
            'version': '1',
        },
        'depends_on': [],
        'document_class': {
            'definition': '$NDIDOCUMENTPATH\/ndi_document_app.json',
            'validation': '$NDISCHEMAPATH\/ndi_document_app_schema.json',
            'class_name': 'ndi_document_app',
            'property_list_name': 'app',
            'class_version': 1,
            'superclasses': [{
                'definition': '$NDIDOCUMENTPATH\/base_document.json'
            }],
        },
        'app': {
            'a': 'b',
            'c': 'd',
        },
    },
    {
        'base': {
            'id': '2',
            'session_id': '2387492',
            'name': 'C',
            'datestamp': '2020-10-28T08:12:20+0000',
            'version': '1',
        },
        'depends_on': [],
        'document_class': {
            'definition': '$NDIDOCUMENTPATH\/ndi_document_app.json',
            'validation': '$NDISCHEMAPATH\/ndi_document_app_schema.json',
            'class_name': 'ndi_document_app',
            'property_list_name': 'app',
            'class_version': 1,
            'superclasses': [{
                'definition': '$NDIDOCUMENTPATH\/base_document.json'
            }],
        },
        'app': {
            'a': 'b',
            'c': 'd',
        },
    },
]

@pytest.fixture
def db():
    database = SQL(
        'postgres://postgres:password@localhost:5432/did_tests', 
        hard_reset_on_init = True,
        debug_mode = False,
    )
    yield database

@pytest.fixture
def mocdocs():
    # names are 'A', 'B', and 'C'
    yield [DIDDocument(data) for data in mock_document_data]

@pytest.fixture
def doc_count(db):
    return lambda db: list(db.execute('select count(*) from (select * from document) src;'))[0][0]

class TestLookupCollection:
    def test_document_collection_creation(self, db):
        results = list(db.execute("""
            SELECT 
                table_name, 
                column_name, 
                data_type 
            FROM 
                information_schema.columns
            WHERE
                table_name = 'document';
        """))
        expected = [
            ('document', 'document_id', 'character varying'),
            ('document', 'data', 'jsonb'),
        ]
        for row in expected:
            assert row in results

    def test_add(self, db, mocdocs, doc_count):
        assert doc_count(db) is 0
        
        db.add(mocdocs[0], save=True)
        
        result = next(db.execute('SELECT document_id FROM document;'))[0]
        assert result is mocdocs[0].id
        result = next(db.execute('SELECT data FROM document;'))[0]
        assert result == mocdocs[0].serialize()

    def test_save(self, db, mocdocs, doc_count):
        assert db.current_transaction is None
        assert doc_count(db) is 0
        db.add(mocdocs[0], save=True)
        assert db.current_transaction is None
        assert doc_count(db) is 1

        db.add(mocdocs[1])
        assert db.current_transaction.is_active
        assert doc_count(db) is 1
        db.add(mocdocs[2])
        assert doc_count(db) is 1

        db.save()
        assert db.current_transaction is None
        assert doc_count(db) is 3

    def test_revert(self, db, mocdocs, doc_count):
        assert db.current_transaction is None
        assert doc_count(db) is 0
        for doc in mocdocs:
            db.add(doc)
            assert doc_count(db) is 0
        assert db.current_transaction.is_active
        db.revert()
        assert db.current_transaction is None
        assert doc_count(db) is 0

        db.add(mocdocs[0], save=True)
        assert db.current_transaction is None
        assert doc_count(db) is 1
