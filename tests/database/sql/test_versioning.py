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

from copy import deepcopy

mock_document_data = [
    {
        'base': {
            'id': '0',
            'session_id': '2387492',
            'name': 'A',
            'datestamp': '2020-10-28T08:12:20+0000',
            'versions': [],
        },
        'depends_on': [],
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
            'versions': [],
        },
        'depends_on': [],
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
            'versions': [],
        },
        'depends_on': [],
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
            WHERE snapshot_id = {snapshot_id};
        """)).hash

        expected_commit_hash = hash_commit(snapshot_hash)

        # check new commit
        new_commit = next(did.database.execute(f"""
            SELECT * FROM commit
            WHERE snapshot_id = {snapshot_id};
        """))
        assert new_commit.snapshot_id == snapshot_id
        assert not new_commit.parent
        assert check_time_format(new_commit.timestamp)
        assert new_commit.hash == expected_commit_hash

        # check new ref
        new_ref = next(did.database.execute(f"""
            SELECT * FROM ref
            WHERE commit_hash = '{new_commit.hash}';
        """))
        assert new_ref.name == 'CURRENT'

    def test_commit_from_existing_ref(self, did, mocdocs, doc_count):
        assert not did.database.current_ref

        did.add(mocdocs[0])
        first_doc_hash = hash_document(mocdocs[0])
        first_snapshot_id = did.database.working_snapshot_id
        did.save()
        first_commit = did.database.current_ref.commit_hash

        did.add(mocdocs[1])
        second_doc_hash = hash_document(mocdocs[1])
        second_snapshot_id = did.database.working_snapshot_id
        did.save()
        second_commit = did.database.current_ref.commit_hash

        # check documents were staged correctly
        snapshot_documents = list(did.database.execute(f"""
            SELECT * FROM snapshot_document;
        """))
        expected_snapshot_documents = [
            (first_snapshot_id, first_doc_hash),
            (second_snapshot_id, first_doc_hash),
            (second_snapshot_id, second_doc_hash),
        ]
        print(snapshot_documents)
        print(expected_snapshot_documents)
        assert snapshot_documents == expected_snapshot_documents

        # check commit was added correctly
        latest_commit = next(did.database.execute(f"""
            SELECT * FROM commit
            WHERE hash = '{second_commit}';
        """))
        assert latest_commit.parent == first_commit
        assert latest_commit.snapshot_id == second_snapshot_id 

        # check ref was updated
        assert did.database.current_ref.commit_hash == second_commit

    def test_get_history(self, did, mocdocs):
        history = did.database.get_history()
        for doc in mocdocs:
            did.add(doc, save=True)
        did.database.upsert_ref('branch-name', did.database.current_ref.commit_hash)
        history = did.database.get_history()

        all_commits = [row.hash for row in (did.database.execute(f"""
            SELECT * FROM commit;
        """))]
        for commit in history:
            assert commit[1] in all_commits

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

    def test_find(self, did, mocdocs):
        pass

    def test_update(self, did, mocdocs):
        doc = mocdocs[0]
        did.add(doc, save=True)
        doc.data['app']['c'] = True
        did.update(doc, save=True)

        current_documents = list(did.database.execute('SELECT document_hash FROM snapshot_document;'))
        assert len(current_documents) == 2

        current_documents = [
            { 'app': doc.data['app'], 'versions': doc.data['base']['versions'] }
            for doc in did.database.execute(f"""
                SELECT * FROM document
                WHERE document_id = '{doc.id}';
            """)
        ]

        assert current_documents == [
            {'app': {'a': True, 'b': True}, 'versions': [1]}, 
            {'app': {'a': True, 'b': True, 'c': True}, 'versions': [2, 1]}
        ]

    def test_upsert(self, did, mocdocs):
        doc = mocdocs[0]
        did.upsert(doc, save=True)

        current_documents = list(did.database.execute('SELECT document_hash FROM snapshot_document;'))
        assert len(current_documents) == 1

        doc.data['app']['c'] = True
        did.upsert(doc, save=True)

        current_documents = list(did.database.execute('SELECT document_hash FROM snapshot_document;'))
        assert len(current_documents) == 2

        current_documents = [
            { 'app': doc.data['app'], 'versions': doc.data['base']['versions'] }
            for doc in did.database.execute(f"""
                SELECT * FROM document
                WHERE document_id = '{doc.id}';
            """)
        ]

        assert current_documents == [
            {'app': {'a': True, 'b': True}, 'versions': [1]}, 
            {'app': {'a': True, 'b': True, 'c': True}, 'versions': [2, 1]}
        ]

    def test_update_by_id(self, did, mocdocs):
        doc = mocdocs[0]
        did.add(doc, save=True)
        updates = { 'app': {'a': True, 'b': True, 'c': True} }
        did.update_by_id(doc.id, document_updates=updates, save=True)

        results = list(did.database.execute('SELECT document_hash FROM snapshot_document;'))
        assert len(results) == 2

        results = [
            { 'app': doc.data['app'], 'versions': doc.data['base']['versions'] }
            for doc in did.database.execute(f"""
                SELECT * FROM document
                WHERE document_id = '{doc.id}';
            """)
        ]

        assert results == [
            {'app': {'a': True, 'b': True}, 'versions': [1]}, 
            {'app': {'a': True, 'b': True, 'c': True}, 'versions': [2, 1]}
        ]

    def test_update_many(self, did, mocdocs):
        for doc in mocdocs:
            did.add(doc)
        did.save()

        by_app_a = Q('app.a') == True
        updates = { 'app': { 'c': False } }

        current_documents = list(did.database.execute('SELECT document_hash FROM snapshot_document;'))
        assert len(current_documents) == 3

        did.update_many(by_app_a, updates, save=True)

        current_documents = list(did.database.execute('SELECT document_hash FROM snapshot_document;'))
        assert len(current_documents) == 6

        current_documents = did.find()

        for doc in current_documents:
            if doc.data['app']['a']:
                assert doc.data['app']['c'] == False
            else:
                try:
                    doc.data['app']['c']
                    assert False
                except KeyError:
                    pass

    def test_delete(self, did, mocdocs):
        """DID.delete is a wrapper for DID.delete_by_id"""
        doc = mocdocs[0]

        assert len(did.find()) == 0

        did.add(doc, save=True)
        assert len(did.find()) == 1

        did.delete(doc, save=True)
        assert len(did.find()) == 0

        current_documents = list(did.database.execute('SELECT document_hash FROM snapshot_document;'))
        assert len(current_documents) == 1

    def test_delete_many(self, did, mocdocs):
        """DID.delete is a wrapper for DID.delete_by_id"""
        assert len(did.find()) == 0

        for doc in mocdocs:
            did.add(doc)
        did.save()
        assert len(did.find()) == 3

        by_app_a = Q('app.a') == True
        did.delete_many(by_app_a, save=True)
        assert len(did.find()) == 1

        current_documents = list(did.database.execute('SELECT document_hash FROM snapshot_document;'))
        assert len(current_documents) == 4