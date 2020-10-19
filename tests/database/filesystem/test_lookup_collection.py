from ndi.database.file_system import LookupCollection
from pathlib import Path
import pytest
from tests.utils import rmrf

@pytest.fixture
def lookup_collection():
    collection = LookupCollection('./test_db', 'document')
    (collection.collection_dir / 'hello:world').touch(exist_ok=False)
    yield collection
    rmrf(collection.collection_dir)

class TestLookupCollection:
    def test_collection_creation(self, lookup_collection):
        collection = lookup_collection

        # Test that collection was created
        assert collection.collection_dir.exists()
        assert collection.collection_dir.is_dir()
    
    def test_add(self, lookup_collection):
        collection = lookup_collection

        collection.add('dependent', 'dependency')

        # Test that lookup file was created
        assert (collection.collection_dir / 'dependent:dependency').exists()
        assert (collection.collection_dir / 'dependent:dependency').is_file()

        # Test that a dependent/dependency pair cannot be added more than once
        with pytest.raises(FileExistsError):
            collection.add('dependent', 'dependency')

    def test_remove(self, lookup_collection):
        collection = lookup_collection

        collection.remove('hello', 'world')

        # Test that specifed relationship not longer exists
        assert not (collection.collection_dir / 'hello:world').exists()
    
    def test_find_dependencies(self, lookup_collection):
        collection = lookup_collection

        results = collection.find_dependencies('hello')

        # Test that list of dependency ids are returned
        assert results == ['world']


    def test_find_dependents(self, lookup_collection):
        collection = lookup_collection

        results = collection.find_dependents('world')

        # Test that list of dependent ids are returned
        assert results == ['hello']
