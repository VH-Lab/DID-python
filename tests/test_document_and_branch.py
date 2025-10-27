import unittest
import os
import numpy as np
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
        branch_g, branch_node_names = make_tree(1, 3, 0.8, 5)

        # Create all branches first
        for i, branch_name in enumerate(branch_node_names):
            if not self.db.do_get_branch_ids() or branch_name not in self.db.do_get_branch_ids():
                 parent_indices = np.where(branch_g[:,i])[0]
                 parent = branch_node_names[parent_indices[0]] if len(parent_indices) > 0 else ''
                 if parent and (not self.db.do_get_branch_ids() or parent not in self.db.do_get_branch_ids()):
                     self.db.add_branch(parent, '') # Add parent with no parent if it doesn't exist
                 self.db.add_branch(branch_name, parent)

        doc_structs = {}
        for i, branch_name in enumerate(branch_node_names):
            doc_struct = {}
            doc_struct['g'], doc_struct['node_names'], doc_struct['docs'] = make_doc_tree([10, 10, 10])
            self.db.set_branch(branch_name)
            self.db.add_docs(doc_struct['docs'])
            doc_structs[branch_name] = doc_struct

        for i, branch_name in enumerate(branch_node_names):
            self.db.set_branch(branch_name)
            doc_struct = doc_structs[branch_name]
            b, msg = verify_db_document_structure(self.db, doc_struct['g'], doc_struct['docs'])
            self.assertTrue(b, msg)

if __name__ == '__main__':
    unittest.main()
