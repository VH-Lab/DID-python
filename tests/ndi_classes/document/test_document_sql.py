import pytest
from types import MethodType
from ndi import Document, Query as Q
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from ndi.database import SQL

def fix_get(Session):
    mock_get = Session.boundary['get']
    def get(db_items, id_):
        for item in db_items:
            if id_ == item.id:
                return item
    Session.boundary['get'] = get
    return Session

def add_delete_tracker(Session):
    Session.delete_tracker = []
    def delete(self, document):
        print('SESSION DELETE')
        self.Session.delete_tracker.append(document)
    Session.delete = delete
    return Session

@pytest.fixture
def new_sql_db():
    Session = fix_get(add_delete_tracker(UnifiedAlchemyMagicMock()))
    db = SQL('nonexistant_db', mock_session=Session)
    yield db

def with_doc_in_db(new_sql_db):
    db = new_sql_db
    doc = Document({ "data": "yeah" })
    db.add(doc)
    return db, doc

def add_dependencies(doc):
    deps = [(key, Document({ 'mynameis': key })) 
            for key in ['a', 'b', 'c']]
    for key, dep in deps:
        doc.add_dependency(dep, key)
    return deps


class TestNdiDocument_SQL:
    def test_doc_gets_ctx_on_add(self, new_sql_db):
        """ndi.database.sql.SQL.add"""
        db = new_sql_db

        # newly instantiated documents do not have a database in context
        doc = Document({ "data": "yeah" })
        assert doc.ctx.db is None

        # documents get assigned a ctx.db on add,
        #   and the ctx has the database instance it was added to
        db.add(doc)
        assert hasattr(doc.ctx, 'db')
        assert doc.ctx.db == db

    def test_doc_gets_ctx_db_on_find(self, new_sql_db):
        """ndi.database.sql.SQL.find"""
        db, doc = with_doc_in_db(new_sql_db)

        # documents retrieved from the database on find get assigned the ctx.database
        found_doc = db.find(Q('id') == doc.id)[0]
        assert isinstance(found_doc, Document)
        assert found_doc.ctx.db == db
        
    def test_doc_gets_ctx_db_on_find_by_id(self, new_sql_db):
        """ndi.database.sql.SQL.find_by_id"""
        db, doc = with_doc_in_db(new_sql_db)

        # documents retrieved from the database on find_by_id get assigned the ctx.database
        found_doc = db.find_by_id(doc.id)
        assert isinstance(found_doc, Document)
        assert found_doc.ctx.db == db

    def test_initial_version(self, new_sql_db):
        """ndi.Document.__init__"""
        db = new_sql_db
        doc = Document({ "data": "dummy" })

        # documents are initialized with version metadata
        metadata = doc.data['_metadata']
        assert metadata['latest_version'] == True
        assert metadata['version_depth'] == 0
        assert metadata['asc_path'] == ''

    def test_save_updates(self, new_sql_db):
        """ndi.Document.save_updates"""
        db, doc = with_doc_in_db(new_sql_db)
        v0_id = doc.id
        dep = Document({})
        doc.add_dependency(dep, key='mock')
        print(doc.data)

        # document version metadata is updated on save
        doc.save_updates()
        metadata = doc.data['_metadata']
        assert metadata['latest_version'] == True
        assert metadata['version_depth'] == 1
        assert metadata['asc_path'] == f',{v0_id}'
        
        # updated document has no dependencies
        assert not any(doc.data['_dependencies'])

        # old version of document in database is no longer latest_version
        doc_v0 = db.find_by_id(v0_id)
        metadata = doc_v0.data['_metadata']
        assert metadata['latest_version'] == False
        assert metadata['version_depth'] == 0
        assert metadata['asc_path'] == ''

        # old version of document retains its dependencies
        assert doc_v0.data['_dependencies']['mock'] == dep.id
    
    def test_get_history(self, new_sql_db):
        """ndi.Document.get_history"""
        db, doc = with_doc_in_db(new_sql_db)
        v0_id = doc.id
        doc.save_updates()
        v1_id = doc.id
        doc.save_updates()
        v2_id = doc.id

        # get history gets all previous versions of document from oldest to newest
        history = doc.get_history()
        metadata = history[0].data['_metadata']
        assert metadata['latest_version'] == False
        assert metadata['version_depth'] == 0
        assert metadata['asc_path'] == ''
        metadata = history[1].data['_metadata']
        assert metadata['latest_version'] == False
        assert metadata['version_depth'] == 1
        assert metadata['asc_path'] == f',{v0_id}'

    def test_add_dependency(self, new_sql_db):
        """ndi.Document.add_dependency"""
        db, doc = with_doc_in_db(new_sql_db)

        dep = Document({ "data": "yep" })
        doc.add_dependency(dep, key='dep')

        # document's dependencies list is updated
        assert doc.data['_dependencies']['dep'] == dep.id

        # dependency has been added to the database
        assert db.find_by_id(dep.id)

        # fails when new doc is already a dependency
        with pytest.raises(RuntimeError):
            doc.add_dependency(dep, key='dup_dep')

        # fails when new doc is added under a key that is already in use
        with pytest.raises(RuntimeError):
            also_a_dep = Document({ "data": "certainly "})
            doc.add_dependency(also_a_dep, key='dep')

    def test_get_dependencies(self, new_sql_db):
        """ndi.Document.get_dependencies"""
        db, doc = with_doc_in_db(new_sql_db)
        #setup dependencies
        deps = [(key, Document({ 'mynameis': key })) 
                for key in ['a', 'b', 'c']]
        for key, dep in deps:
            doc.add_dependency(dep, key)
        
        # before get_dependencies, _dependencies values are ids
        doc_dependencies = doc.data['_dependencies']
        for key, dep in deps:
            assert doc_dependencies[key] == dep.id
        
        # after get_dependencies, _dependencies values are ndi_document instances
        doc.get_dependencies()
        doc_dependencies = doc.data['_dependencies']
        for key, dep in deps:
            assert isinstance(doc_dependencies[key], Document)
            assert doc_dependencies[key].id == dep.id

    def test_delete(self, new_sql_db):
        """ndi.Document.get_dependencies"""
        db, doc = with_doc_in_db(new_sql_db)
        # setup dependencies and history
        v0_deps = add_dependencies(doc)
        v0_id = doc.id
        doc.save_updates()
        v1_deps = add_dependencies(doc)
        v1_id = doc.id

        assert db.find_by_id(doc.id)

        doc.delete(force=True)

        get_deleted(db)

def get_deleted(db):
    calls = [call for call in db.Session._mock_mock_calls if 'delete' in str(call)]

    for c in calls: 
        print(c)
        print(c.__dict__)
    # print(calls)