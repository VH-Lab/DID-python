from did.database import Mongo
from did.versioning import hash_document
from did import DIDDocument, DID, Query
import os
import pytest
import pickle


@pytest.fixture
def did():
    driver = Mongo(connection_string="mongodb://localhost:27017",
                 database="test_did",
                 versioning_collection="version",
                 document_collection="documents",
                 hard_reset_on_init=True)
    binary_directory = "./"
    return DID(driver, binary_directory)


@pytest.fixture
def mock_documents():
    test_dir = os.path.split(os.path.abspath(__file__))[0]
    mock_document_path = os.path.join(test_dir, 'mock.bin')
    with open(mock_document_path, 'rb') as f:
        docs = pickle.load(f)
    return docs


class TestMongoDB:
    def test_nomral_operations(self, did, mock_documents):
        apps = [doc for doc in mock_documents if 'app' in doc.data and doc.data['app']['os'] == 'Linux']
        for app in apps:
            did.add(app)
        assert len(did.find(Query('base.id').match(".*"))) == len(apps)
        snapshot_id = did.driver.working_snapshot_id
        did.save("Added apps whos os is Linux")
        assert len(did.get_history()) == 2
        assert did.get_history()[0]['message'] == "Added apps whos os is Linux"
        assert did.get_history()[0]['snapshot_id'] == snapshot_id
        doc = did.find_by_id("412684679964c16b_40aab8dbe86d6093")[0]
        did.delete_by_id("412684679964c16b_40aab8dbe86d6093")
        assert did.find_by_hash(hash_document(doc)) == None
        assert did.find_by_id("412684679964c16b_40aab8dbe86d6093") == []
        assert did.find(Query('base.name') == 'app #7') == []
        did.revert()
        assert did.find_by_hash(hash_document(doc)).data == doc.data
        assert did.find_by_id("412684679964c16b_40aab8dbe86d6093")[0].data == doc.data
        assert did.find(Query('base.name') == 'app #7')[0].data == doc.data
        try:
            did.add(doc)
            assert True == False
        except:
            pass
        doc.data['base']['name'] = 'new app name'
        did.upsert(doc)
        assert did.find(Query('base.name') == 'new app name')[0].data == doc.data
        did.save("Update app # 7 to new app name")
        assert(len(did.get_history())) == 3
        assert(did.get_history()[0]['message'] == "Update app # 7 to new app name")
    
    def test_search_heavy_operations(self, did, mock_documents):
        windows = [doc for doc in mock_documents if 'app' in doc.data and doc.data['app']['os'] == 'Windows']
        mac_os = [doc for doc in mock_documents if 'app' in doc.data and doc.data['app']['os'] == 'Mac OS']
        linux = [doc for doc in mock_documents if 'app' in doc.data and doc.data['app']['os'] == 'Linux']
        for app in windows:
            did.add(app)
        snapshot1_id = did.db.working_snapshot_id
        docs = did.find(Query("app.os") == 'Windows')
        assert len(docs) == len(windows)
        did.save()
        assert len(did.find(Query("app.os") == 'Windows', snapshot=snapshot1_id)) == len(windows)
        assert len(did.find(Query("app.os") == 'Windows', snapshot=snapshot1_id, commit="1234563")) == len(windows)
        for app in mac_os:
            did.add(app)
        docs = did.find(Query("app.os") == 'Linux')
        assert len(docs) == 0
        docs = did.find(Query("app.os") == 'Windows')
        assert len(docs) == len(windows)
        for app in windows:
            did.delete(app)
        did.save()
        docs = did.find(Query("app.os") == 'Windows')
        assert len(docs) == 0
        docs = did.find(Query("app.os") == 'Windows', in_all_history=True)
        assert len(docs) == len(windows)
        history = did.get_history()
        commit_hash = history[1]['commit_hash']
        docs = did.find(Query("app.os") == 'Windows', commit=commit_hash)
        assert len(docs) == len(windows)
        for app in windows:
            did.upsert(app)
        for app in mac_os:
            did.upsert(app)
        for app in linux:
            did.upsert(app)
        assert len(did.find(Query("app.os") == 'Windows')) == len(windows)
        assert len(did.find(Query("app.os") == 'Mac OS')) == len(mac_os)
        assert len(did.find(Query("app.os") == 'Linux')) == len(linux)
        did.revert()
        assert len(did.find(Query("app.os") == 'Windows')) == 0
        assert len(did.find(Query("app.os") == 'Mac OS')) == len(mac_os)
        assert len(did.find(Query("app.os") == 'Linux')) == 0