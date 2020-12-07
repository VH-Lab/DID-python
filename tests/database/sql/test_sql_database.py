from did import DID, Query as Q
from did.database.sql import SQL
from did.document import DIDDocument
from did.versioning import hash_document
import pytest

from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker

from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker

from copy import deepcopy

mock_document_data = [
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
            'snapshots': [],
            'records': [],
        },
        'depends_on': [],
        'dependencies': [],
        'binary_files': [],
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
            'snapshots': [],
            'records': [],
        },
        'depends_on': [],
        'dependencies': [],
        'binary_files': [],
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
def did():
    did = DID(
        database = SQL(
            'postgres://postgres:password@localhost:5432/did_versioning_tests', 
            hard_reset_on_init = True,
            debug_mode = False,
            verbose_feedback = False,
        ),
        binary_directory='./tests/database/sql/test_versioning_binary_data',
    )
    yield did
    did.database.connection.close()


@pytest.fixture
def mocdocs():
    # names are 'A', 'B', and 'C'
    yield [DIDDocument(data) for data in deepcopy(mock_document_data)]

@pytest.fixture
def doc_count(did):
    return lambda did: len(list(did.find()))

class TestSqlDatabase:
    def test_document_collection_creation(self, did):
        results = list(did.db.execute("""
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

    def test_add(self, did, mocdocs, doc_count):
        assert doc_count(did) is 0
        
        did.add(mocdocs[0])
        did.save()
        
        result = next(did.db.execute('SELECT document_id FROM document;'))[0]
        assert result is mocdocs[0].id
        result = next(did.db.execute('SELECT data FROM document;'))[0]
        assert result == mocdocs[0].data

    def test_save(self, did, mocdocs, doc_count):
        assert did.db.current_transaction is None
        assert doc_count(did) is 0
        did.add(mocdocs[0])
        did.save()
        assert did.db.current_transaction is None
        assert doc_count(did) is 1

        did.add(mocdocs[1])
        assert did.db.current_transaction.is_active
        assert doc_count(did) is 2
        did.add(mocdocs[2])
        assert doc_count(did) is 3

        did.save()
        assert did.db.current_transaction is None
        assert doc_count(did) is 3

    def test_revert(self, did, mocdocs, doc_count):
        assert did.db.current_transaction is None
        assert doc_count(did) is 0
        for doc in mocdocs:
            did.add(doc)
        assert doc_count(did) is 3
        assert did.db.current_transaction.is_active
        did.revert()
        assert did.db.current_transaction is None
        assert doc_count(did) is 0

        did.add(mocdocs[0])
        did.save()
        assert did.db.current_transaction is None
        assert doc_count(did) is 1

    def test_generate_sqla_filter(self, did):
        by_name = Q('base.name') == 'A'
        assert str(did.db.generate_sqla_filter(by_name)) == '(document.data #>> :data_1) = :param_1'

        by_name_or_class_name = by_name \
            | (Q('document_class.class_name') == 'ndi_document_app')
        assert str(did.db.generate_sqla_filter(by_name_or_class_name)) == '(document.data #>> :data_1) = :param_1 OR (document.data #>> :data_2) = :param_2'
    
    def test_find(self, did, mocdocs):
        # test SELECT where there are no results
        results = did.find()
        assert results == []

        for doc in mocdocs:
            did.add(doc)
        did.save()

        # test `SELECT *`
        found_ids = [doc.id for doc in did.find()]
        expected_ids = [doc.id for doc in mocdocs]
        assert len(found_ids) == len(expected_ids)
        for found_id in found_ids:
            assert found_id in expected_ids

        # test select by some query
        by_version = Q('app.a') == True
        found_ids = [doc.id for doc in did.find(by_version)]
        expected_ids = [doc.id for doc in mocdocs if doc.data['app']['a'] == True]
        assert len(found_ids) == len(expected_ids)
        for found_id in found_ids:
            assert found_id in expected_ids

        # test select by some composite query
        by_version = Q('app.a') == True
        by_version_and_name = by_version \
            & (Q('base.name') == 'A')
        found_ids = [doc.id for doc in did.find(by_version_and_name)]
        expected_ids = [
            doc.id for doc in mocdocs 
            if doc.data['app']['a'] == True
            and doc.data['base']['name'] == 'A'
        ]
        assert len(found_ids) == len(expected_ids)
        for found_id in found_ids:
            assert found_id in expected_ids

    def test_find_by_id(self, did, mocdocs):
        for doc in mocdocs:
            did.add(doc)
        did.save()

        assert did.find_by_id('0').id == '0'

    def test_find_by_hash(self, did, mocdocs):
        for doc in mocdocs:
            did.add(doc)
        did.save()

        hash_ = hash_document(mocdocs[0])

        assert did.find_by_hash(hash_).id == '0'

    def test_update(self, did, mocdocs):
        for doc in mocdocs:
            did.add(doc)
        did.save()

        mocdocs[0].data['app']['b'] = False
        mocdocs[0].data['app']['c'] = False
        did.update(mocdocs[0], save=True)

        updated_data = did.find_by_id(mocdocs[0].id).data['app']
        expected_data = mocdocs[0].data['app']
        assert updated_data == expected_data

    def test_update_by_id(self, did, mocdocs):
        for doc in mocdocs:
            did.add(doc)
        did.save()

        updates = { 'app': {
                'a': False,
                'c': True,
        } }
        did.update_by_id(mocdocs[0].id, updates)
        did.save()
        merged_data = did.find_by_id(mocdocs[0].id).data['app']
        expected_data = {
            **mocdocs[0].data['app'],
            **updates['app']
        }
        assert merged_data == expected_data

    def test_update_many(self, did, mocdocs):
        for doc in mocdocs:
            did.add(doc)
        did.save()

        updates = { 'app': {
                'a': False,
                'c': True,
        } }
        query = Q('app.b') == False
        did.update_many(query=query, document_updates=updates)
        did.save()
        merged_data = [doc.data['app'] for doc in did.find()]
        expected_data = [
            (
                {
                    **doc.data['app'],
                    **updates['app'],
                } 
                if doc.data['app']['b'] == False 
                else doc.data['app']
            )
            for doc in mocdocs
        ]
        assert len(merged_data) == len(expected_data)
        for expected in expected_data:
            assert expected in merged_data
    
    def test_upsert(self, did, mocdocs, doc_count):
        assert doc_count(did) is 0
        did.upsert(mocdocs[0])
        did.save()
        assert doc_count(did) is 1

        mocdocs[0].data['app']['a'] = False
        did.upsert(mocdocs[0])
        did.save()
        assert doc_count(did) is 1
        updated_data = did.find_by_id(mocdocs[0].id).data
        assert updated_data == mocdocs[0].data
    
    def test_update_dependencies(self, did, mocdocs):
        for doc in mocdocs:
            did.add(doc, save=True)

        document = mocdocs[1]
        hash_ = hash_document(document)
        document.data['dependencies'].append('1234567')
        did.update_dependencies(hash_, document.data['dependencies'], save=True)
        document.data['dependencies'].append('76trfg')
        did.update_dependencies(hash_, document.data['dependencies'], save=True)

        updated_doc = did.find_by_hash(hash_)
        assert updated_doc.data['dependencies'] == ['1234567', '76trfg']

    def test_delete(self, did, mocdocs, doc_count):
        for doc in mocdocs:
            did.add(doc)
        did.save()
        assert doc_count(did) is len(mocdocs)

        did.delete(mocdocs[0])
        did.save()
        assert doc_count(did) is len(mocdocs) - 1

        assert not did.find_by_id(mocdocs[0].id)

    def test_delete_by_id(self, did, mocdocs, doc_count):
        for doc in mocdocs:
            did.add(doc)
        did.save()
        assert doc_count(did) is len(mocdocs)

        did.delete_by_id(mocdocs[0].id)
        did.save()
        assert doc_count(did) is len(mocdocs) - 1

        assert not did.find_by_id(mocdocs[0].id)

    def test_delete_many(self, did, mocdocs, doc_count):
        for doc in mocdocs:
            did.add(doc)
        did.save()
        assert doc_count(did) is len(mocdocs)

        query = Q('app.b') == False
        did.delete_many(query=query)
        did.save()

        expected_doc_count = len([
            1 for doc in mocdocs if doc.data['app']['b'] != False 
        ])

        docs_in_db = did.find()
        assert len(docs_in_db) == expected_doc_count
        assert all(doc.data['app']['b'] != False for doc in docs_in_db)