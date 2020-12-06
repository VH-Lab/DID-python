from did import DID, Query as Q
from did.database.sql import SQL
from did.document import DIDDocument
from did.versioning import hash_document, hash_snapshot, hash_commit
from did.time import check_time_format
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
    yield [DIDDocument(data) for data in mock_document_data]

@pytest.fixture
def doc_count(did):
    return lambda did: list(did.database.execute('select count(*) from (select * from document) src;'))[0][0]

class TestSqlVersioning:
    def test_document_collection_creation(self, did):
        results = list(did.database.execute("""
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
            ('document', 'hash', 'character varying'),
        ]
        for row in results:
            assert row in expected

    def test_initial_snapshot(self, did, mocdocs):
        assert not did.database.current_transaction
        with did.database.transaction_handler() as connection:
            new_snapshot_id = did.database.working_snapshot_id
        did.db.save()
        results = next(did.database.execute(f"""
            SELECT snapshot_id FROM snapshot
            WHERE snapshot_id = {new_snapshot_id};
        """))
        assert results.snapshot_id == 1

    def test_write_snapshot(self, did, mocdocs):
        for doc in mocdocs:
            did.add(doc)
        new_snapshot_id = did.database.working_snapshot_id
        document_hashes = [hash_document(doc) for doc in mocdocs]
        did.save()
        expected_snapshot_hash = hash_snapshot(new_snapshot_id, document_hashes)
        results = next(did.database.execute(f"""
            SELECT hash FROM snapshot
            WHERE snapshot_id = {new_snapshot_id};
        """))
        assert results.hash == expected_snapshot_hash
    
    def test_initial_commit(self, did, mocdocs):
        for doc in mocdocs:
            did.add(doc)
        snapshot_id = did.database.working_snapshot_id
        did.save()

        snapshot_hash = next(did.database.execute(f"""
            SELECT hash FROM snapshot
            WHERE snapshot_id = {new_snapshot_id};
        """)).hash

        expected_commit_hash = hash_commit(snapshot_hash)

        # check new commit
        new_commit = next(did.database.execute(f"""
            SELECT * FROM commit
            WHERE snapshot_id = {new_snapshot_id};
        """))
        assert new_commit.snapshot_id == snapshot_id
        assert not new_commit.parent
        assert check_time_format(new_commit.timestamp)
        assert new_commit.hash == expected_commit_hash

        # check new ref
        new_ref = next(did.database.execute(f"""
            SELECT * FROM ref
            WHERE commit_hash = {new_commit.hash};
        """))
        assert new_ref.name == 'CURRENT'

    def test_add(self, did, mocdocs, doc_count):
        assert doc_count(did) is 0
        
        did.add(mocdocs[0])
        snapshot_id = did.database.working_snapshot_id
        expected_document_hash = hash_document(mocdocs[0])
        did.save()
        
        # document is added to document table
        result = next(did.database.execute('SELECT document_id FROM document;'))[0]
        assert result is mocdocs[0].id
        result = next(did.database.execute('SELECT data FROM document;'))[0]
        assert result == mocdocs[0].data
        result = next(did.database.execute('SELECT hash FROM document;'))[0]
        assert result == expected_document_hash

        # document is added to JOIN table
        result = next(did.database.execute('SELECT document_hash FROM snapshot_document;'))[0]
        assert result == expected_document_hash

    