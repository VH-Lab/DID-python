from did.database.mongo import _TransactionHandler
import pytest


@pytest.fixture
def throwException():
    def func():
        raise RuntimeError("An Exception Has Occured!!")
    return func

class TestTransactionHandler:
    def test_normal_operation(self):
        map = {}
        with _TransactionHandler() as session:
            session.action_on(map, dict.__setitem__, ['joe', 1],
                            dict.pop, ['joe'])
            assert map['joe'] == 1
            session.action_on(None, lambda x: x+2, [5], None, None)
            assert session.query_return_value(-1, immediate=True)==7
            session.action_on(map, dict.__setitem__, ['john', 2],
                            dict.pop, ['john'], immediately_executed=False)
            assert 'john' not in map
        assert 'john' in map

    def test_normal_operation_with_exception(self, throwException):
        map = {}
        with _TransactionHandler(nothrow=True) as session:
            session.action_on(map, dict.__setitem__, ['joe', 1],
                            dict.pop, ['joe'])
            session.action_on(None, lambda x: x+2, [5], None, None)
            session.action_on(map, dict.__setitem__, ['john', 2],
                            dict.pop, ['john'], immediately_executed=False)
            assert map == {'joe' : 1}
            session.action_on(None, throwException, None, None, None)
        assert map == {}
        
        try:
            with _TransactionHandler(nothrow=False) as session:
                session.action_on(map, dict.__setitem__, ['joe', 1],
                                dict.pop, ['joe'])
                session.action_on(None, lambda x: x+2, [5], None, None)
                session.action_on(map, dict.__setitem__, ['john', 2],
                                dict.pop, ['john'], immediately_executed=False)
                session.action_on(None, throwException, None, None, None)
        except RuntimeError:
            assert map == {}

    def test_twophase_transaction_fail_nothrow(self, throwException):
        map = {}
        parent = _TransactionHandler(nothrow=True)
        with parent:
            parent.action_on(map, dict.__setitem__, ['joe', 1],
                            dict.pop, ['joe'])
            assert map == {'joe' : 1}
            with _TransactionHandler(parent=parent, nothrow=True) as session:
                session.action_on(map, dict.__setitem__, ['john', 2],
                                dict.pop, ['john'])
                assert map == {'joe' : 1, 'john' : 2}
                session.action_on(None, throwException, None, None, None)
            assert map =={'joe' : 1}
        assert map == {'joe' : 1}

    
    def test_twophase_transaction_fail(self, throwException):
        map = {}
        parent = _TransactionHandler()
        with parent:
            parent.action_on(map, dict.__setitem__, ['joe', 1],
                            dict.pop, ['joe'])
            assert map == {'joe' : 1}
            with _TransactionHandler(parent=parent) as session:
                session.action_on(map, dict.__setitem__, ['john', 2],
                                dict.pop, ['john'])
                assert map == {'joe' : 1, 'john' : 2}
                session.action_on(None, throwException, None, None, None)
            assert map =={'joe' : 1}
        assert map == {'joe' : 1}

    def test_twophase_transaction_both_fail(self, throwException):
        map = {}
        parent = _TransactionHandler(throwException)
        try:
            with parent:
                parent.action_on(map, dict.__setitem__, ['joe', 1],
                                dict.pop, ['joe'])
                with _TransactionHandler(parent=parent) as session:
                    throwException
                    session.action_on(map, dict.__setitem__, ['john', 2],
                                    dict.pop, ['john'])
                    session.action_on(None, throwException, None, None, None)
                throwException()
        except:
            assert map == {}

    def test_twophase_both_success(self):
        map = {}
        parent = _TransactionHandler()
        with parent:
            parent.action_on(map, dict.__setitem__, ['joe', 1],
                            dict.pop, ['joe'])
            assert map == {'joe' : 1}
            with _TransactionHandler(parent=parent) as session:
                session.action_on(map, dict.__setitem__, ['john', 2],
                                dict.pop, ['john'])
                assert map == {'joe' : 1, 'john' : 2}
            assert map =={'joe' : 1, 'john' : 2}
        assert map == {'joe' : 1, 'john' : 2}

    def test_commit(self):
        map = {}
        parent = _TransactionHandler()
        with parent:
            parent.action_on(map, dict.__setitem__, ['joe', 1],
                            dict.pop, ['joe'])
            assert map == {'joe' : 1}
            with _TransactionHandler(parent=parent) as session:
                session.action_on(map, dict.__setitem__, ['john', 2],
                                dict.pop, ['john'])
                assert map == {'joe' : 1, 'john' : 2}
                session.commit = False
            assert map =={'joe' : 1}
            parent.commit = False
        assert map == {}       
        

        
