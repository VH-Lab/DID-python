from ndi.database import FileSystem
from ndi import Document, Query as Q
from pathlib import Path
import pytest
from tests.utils import rmrf


@pytest.fixture
def fs_database():
    database = FileSystem('./test_db')
    test_doc = Document(
        data={'y': 'mx + b'},
        name='line',
        type_='test'
    )
    (database.collection_dir / f'{test_doc.id}.dat').write_bytes(test_doc.serialize())
    yield database, test_doc
    rmrf(database.db_dir)


class TestFileSystem:
    def test_database_creation(self, fs_database):
        database, _ = fs_database

        # Test that database directory was created
        assert database.db_dir.exists()
        assert database.db_dir.is_dir()

    def test_add(self, fs_database):
        database, test_doc = fs_database
        doc = Document(
            data={'PV': 'nRT'},
            name='gas law',
            type_='test'
        )

        database.add(doc)

        # Test that document was added to database
        assert (database.collection_dir / f'{doc.id}.dat').exists()
        assert (database.collection_dir / f'{doc.id}.dat').is_file()

        # Test that error is raised when adding a document that already exists
        with pytest.raises(FileExistsError):
            database.add(test_doc)

    def test_update(self, fs_database):
        database, test_doc = fs_database

        test_doc.data['y'] = 'f(x)'
        database.update(test_doc, force=True)
        rebuilt_doc = Document.from_flatbuffer((database.collection_dir / f'{test_doc.id}.dat').read_bytes())

        # Test that the saved document was updated in the database
        assert rebuilt_doc.data['y'] == test_doc.data['y']

        # That that error is raised when updating a document that doesn't exist
        with pytest.raises(FileNotFoundError):
            doc = Document(
                data={'PV': 'nRT'},
                name='gas law',
                type_='test'
            )
            database.update(doc, force=True)

    def test_upsert(self, fs_database):
        database, test_doc = fs_database
        
        doc = Document(
            data={'PV': 'nRT'},
            name='gas law',
            type_='test'
        )
        database.upsert(doc, force=True)

        # Test that document was added to database
        assert (database.collection_dir / f'{doc.id}.dat').exists()
        assert (database.collection_dir / f'{doc.id}.dat').is_file()
        
        test_doc.data['y'] = 'f(x)'
        database.upsert(test_doc, force=True)
        rebuilt_doc = Document.from_flatbuffer((database.collection_dir / f'{test_doc.id}.dat').read_bytes())
       
        # Test that the saved document was updated in the database
        assert rebuilt_doc.data['y'] == test_doc.data['y']
    
    def test_delete(self, fs_database):
        database, test_doc = fs_database

        database.delete(test_doc, force=True)

        # Test that deleted document no longer exists
        assert not (database.collection_dir / f'{test_doc.id}.dat').exists()
    
    def test_find(self, fs_database):
        database, test_doc = fs_database

        results = database.find(Q('y') == 'mx + b')

        # Test that database results are what is expected
        assert type(results[0]) == Document
        assert results[0] == test_doc
    
    def test_update_many(self, fs_database):
        database, test_doc = fs_database

        database.update_many(Q('y') == 'mx + b', {'y': 'f(x)'}, force=True)
        rebuilt_doc = Document.from_flatbuffer((database.collection_dir / f'{test_doc.id}.dat').read_bytes())

        # Test that the document affected was updated in the database
        assert rebuilt_doc.data['y'] == 'f(x)'

    def test_delete_many(self, fs_database):
        database, test_doc = fs_database
        database.delete_many(Q('y') == 'mx + b', force=True)

        # Test that the document affected no longer exists
        assert not (database.collection_dir / f'{test_doc.id}.dat').exists()
    
    def test_find_by_id(self, fs_database):
        database, test_doc = fs_database

        result = database.find_by_id(test_doc.id)

        # Test that database results are what is expected
        assert type(result) == Document
        assert result == test_doc

    def test_update_by_id(self, fs_database):
        database, test_doc = fs_database

        database.update_by_id(test_doc.id, {'y': 'f(x)'}, force=True)
        rebuilt_doc = Document.from_flatbuffer((database.collection_dir / f'{test_doc.id}.dat').read_bytes())

        # Test that the document affected was updated in the database
        assert rebuilt_doc.data['y'] == 'f(x)'
