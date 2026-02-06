import unittest
import os
import networkx as nx
from did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_tree, name_tree, add_branch_nodes, verify_branch_nodes, verify_branch_node_structure, delete_random_branch

class TestBranch(unittest.TestCase):
    DB_FILENAME = 'test_db_branch.sqlite'

    def setUp(self):
        # Create a temporary working directory to run tests in
        self.db = SQLiteDB(self.DB_FILENAME)
        self.g = make_tree(4, 3, 0.8, 10)
        self.node_names = name_tree(self.g)

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)

    def test_add_and_verify_branch_nodes(self):
        add_branch_nodes(self.db, '', self.g, self.node_names)

        b, missing = verify_branch_nodes(self.db, self.g, self.node_names)
        self.assertTrue(b, f"Some branches are missing: {missing}")

        b, msg = verify_branch_node_structure(self.db, self.g, self.node_names)
        self.assertTrue(b, msg)

    def test_random_branch_deletions(self):
        add_branch_nodes(self.db, '', self.g, self.node_names)

        num_random_deletions = min(35, len(self.g.nodes()))

        for _ in range(num_random_deletions):
            self.g, self.node_names = delete_random_branch(self.db, self.g, self.node_names)

        b, msg = verify_branch_node_structure(self.db, self.g, self.node_names)
        self.assertTrue(b, f"After random deletions: {msg}")

if __name__ == '__main__':
    unittest.main()