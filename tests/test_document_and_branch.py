import unittest
import os
import networkx as nx
from did.implementations.sqlitedb import SQLiteDB
from tests.utility_helpers import make_tree
from tests.helpers import make_doc_tree, verify_db_document_structure

class TestDocumentAndBranch(unittest.TestCase):
    DB_FILENAME = 'test_db_docs_and_branch.sqlite'

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.DB_FILENAME):
            os.remove(cls.DB_FILENAME)
        cls.db = SQLiteDB(cls.DB_FILENAME)

    @classmethod
    def tearDownClass(cls):
        cls.db.close_db()
        if os.path.exists(cls.DB_FILENAME):
            os.remove(cls.DB_FILENAME)

    def test_add_branch_nodes(self):
        graph, _ = make_tree(1, 3, 0.8, 5)

        # Create all branches in a valid order (parents before children)
        for i, branch_name in enumerate(nx.topological_sort(graph)):
            parents = list(graph.predecessors(branch_name))
            parent_name = parents[0] if parents else None
            self.db.add_branch(branch_name, parent_branch_id=parent_name)

        doc_structs = {}
        for branch_name in graph.nodes():
            doc_struct = {}
            doc_struct['g'], doc_struct['node_names'], doc_struct['docs'] = make_doc_tree([10, 10, 10])
            self.db.set_branch(branch_name)
            self.db.add_docs(doc_struct['docs'])
            doc_structs[branch_name] = doc_struct

        for branch_name in graph.nodes():
            self.db.set_branch(branch_name)
            doc_struct = doc_structs[branch_name]
            b, msg = verify_db_document_structure(self.db, doc_struct['g'], doc_struct['docs'])
            self.assertTrue(b, msg)

if __name__ == '__main__':
    unittest.main()
