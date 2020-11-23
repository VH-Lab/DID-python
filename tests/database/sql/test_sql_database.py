from did.database.sql import SQL
from did.document import DIDDocument
from did import Query as Q
import pytest

from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker

from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker

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
            'version': '2',
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
            'a': False,
            'b': False
        },
    },
]

@pytest.fixture
def db():
    database = SQL(
        'postgres://postgres:password@localhost:5432/did_tests', 
        hard_reset_on_init = True,
        debug_mode = False,
        verbose_feedback = False,
    )
    yield database

@pytest.fixture
def mocdocs():
    # names are 'A', 'B', and 'C'
    yield [DIDDocument(data) for data in mock_document_data]

@pytest.fixture
def doc_count(db):
    return lambda db: list(db.execute('select count(*) from (select * from document) src;'))[0][0]

class TestSqlDatabase:
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
        
        db.add(mocdocs[0])
        db.save()
        
        result = next(db.execute('SELECT document_id FROM document;'))[0]
        assert result is mocdocs[0].id
        result = next(db.execute('SELECT data FROM document;'))[0]
        assert result == mocdocs[0].data

    def test_save(self, db, mocdocs, doc_count):
        assert db.current_transaction is None
        assert doc_count(db) is 0
        db.add(mocdocs[0])
        db.save()
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

        db.add(mocdocs[0])
        db.save()
        assert db.current_transaction is None
        assert doc_count(db) is 1

    def test_generate_sqla_filter(self, db):
        by_name = Q('base.name') == 'A'
        assert str(db.generate_sqla_filter(by_name)) == '(document.data #>> :data_1) = :param_1'

        by_name_or_class_name = by_name \
            | (Q('document_class.class_name') == 'ndi_document_app')
        assert str(db.generate_sqla_filter(by_name_or_class_name)) == '(document.data #>> :data_1) = :param_1 OR (document.data #>> :data_2) = :param_2'
    
    def test_find(self, db, mocdocs):
        # test SELECT where there are no results
        results = db.find()
        assert results == []

        for doc in mocdocs:
            db.add(doc)
        db.save()

        # test `SELECT *`
        found_ids = [doc.id for doc in db.find()]
        expected_ids = [doc.id for doc in mocdocs]
        assert len(found_ids) == len(expected_ids)
        for found_id in found_ids:
            assert found_id in expected_ids

        # test select by some query
        by_version = Q('base.version') == '1'
        found_ids = [doc.id for doc in db.find(by_version)]
        expected_ids = [doc.id for doc in mocdocs if doc.data['base']['version'] == '1']
        assert len(found_ids) == len(expected_ids)
        for found_id in found_ids:
            assert found_id in expected_ids

        # test select by some composite query
        by_version = Q('base.version') == '1'
        by_version_and_name = by_version \
            & (Q('base.name') == 'A')
        found_ids = [doc.id for doc in db.find(by_version_and_name)]
        expected_ids = [
            doc.id for doc in mocdocs 
            if doc.data['base']['version'] == '1'
            and doc.data['base']['name'] == 'A'
        ]
        assert len(found_ids) == len(expected_ids)
        for found_id in found_ids:
            assert found_id in expected_ids

    def test_find_by_id(self, db, mocdocs):
        for doc in mocdocs:
            db.add(doc)
        db.save()

        assert db.find_by_id('0').id == '0'

    def test_update(self, db, mocdocs):
        for doc in mocdocs:
            db.add(doc)
        db.save()

        mocdocs[0].data['app']['b'] = False
        mocdocs[0].data['app']['c'] = False
        db.update(mocdocs[0])

        updated_data = db.find_by_id(mocdocs[0].id).data
        expected_data = mocdocs[0].data
        assert updated_data == expected_data

    def test_update_by_id(self, db, mocdocs):
        for doc in mocdocs:
            db.add(doc)
        db.save()

        updates = { 'app': {
                'a': False,
                'c': True,
        } }
        db.update_by_id(mocdocs[0].id, updates)
        db.save()
        merged_data = db.find_by_id(mocdocs[0].id).data 
        expected_data = {
            **mocdocs[0].data,
            'app': {
                **mocdocs[0].data['app'],
                **updates['app']
            }
        }
        assert merged_data == expected_data

    def test_update_many(self, db, mocdocs):
        for doc in mocdocs:
            db.add(doc)
        db.save()

        updates = { 'app': {
                'a': False,
                'c': True,
        } }
        query = Q('app.b') == False
        db.update_many(query=query, updates=updates)
        db.save()
        merged_data = [doc.data for doc in db.find()]
        expected_data = [
            (
                {
                    **doc.data,
                    'app': {
                        **doc.data['app'],
                        **updates['app'],
                    }
                } 
                if doc.data['app']['b'] == False 
                else doc.data
            )
            for doc in mocdocs
        ]
        assert merged_data == expected_data
    
    def test_upsert(self, db, mocdocs, doc_count):
        assert doc_count(db) is 0
        db.upsert(mocdocs[0])
        db.save()
        assert doc_count(db) is 1

        mocdocs[0].data['app']['a'] = False
        db.upsert(mocdocs[0])
        db.save()
        assert doc_count(db) is 1
        updated_data = db.find_by_id(mocdocs[0].id).data
        assert updated_data == mocdocs[0].data

    def test_delete(self, db, mocdocs, doc_count):
        for doc in mocdocs:
            db.add(doc)
        db.save()
        assert doc_count(db) is len(mocdocs)

        db.delete(mocdocs[0])
        db.save()
        assert doc_count(db) is len(mocdocs) - 1

        assert not db.find_by_id(mocdocs[0].id)

    def test_delete_by_id(self, db, mocdocs, doc_count):
        for doc in mocdocs:
            db.add(doc)
        db.save()
        assert doc_count(db) is len(mocdocs)

        db.delete_by_id(mocdocs[0].id)
        db.save()
        assert doc_count(db) is len(mocdocs) - 1

        assert not db.find_by_id(mocdocs[0].id)

    def test_delete_many(self, db, mocdocs, doc_count):
        for doc in mocdocs:
            db.add(doc)
        db.save()
        assert doc_count(db) is len(mocdocs)

        query = Q('app.b') == False
        db.delete_many(query=query, deletes=deletes)
        db.save()

        expected_doc_count = len([
            1 for doc in mocdocs if doc.data['app']['b'] == False 
        ])

        docs_in_db = db.find()
        assert len(docs_in_db) == expected_doc_count
        assert all(doc.data['app']['b'] != False for doc in docs_in_db)