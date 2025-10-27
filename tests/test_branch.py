import unittest
import os
import random
import networkx as nx
from did.implementations.sqlitedb import SQLiteDB
from tests.utility_helpers import make_tree

class TestBranch(unittest.TestCase):
    DB_FILENAME = 'branch_test.sqlite'

    def setUp(self):
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)
        self.db = SQLiteDB(self.DB_FILENAME)

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.DB_FILENAME):
            os.remove(self.DB_FILENAME)

    def _add_graph_to_db(self, graph, roots):
        """
        Adds a networkx graph of branches to the database.
        """
        if not roots:
            return

        # Initial branch creation
        self.db.add_branch(roots[0])

        q = roots.copy()
        head = 0
        while head < len(q):
            parent_name = q[head]
            head += 1

            for child_name in graph.successors(parent_name):
                self.db.add_branch(child_name, parent_branch_id=parent_name)
                q.append(child_name)

    def _verify_db_structure(self, graph):
        """
        Verifies that the branch structure in the database matches the graph.
        """
        db_branches = set(self.db.all_branch_ids())
        graph_nodes = set(graph.nodes())
        self.assertEqual(db_branches, graph_nodes, "Mismatch between DB branches and graph nodes.")

        for node in graph.nodes():
            # Verify parent
            db_parent = self.db.get_branch_parent(node)
            if not db_parent:  # Treat '' as None
                db_parent = None
            graph_parents = list(graph.predecessors(node))
            graph_parent = graph_parents[0] if graph_parents else None
            self.assertEqual(db_parent, graph_parent, f"Parent mismatch for node {node}")

            # Verify children
            db_children = set(self.db.get_sub_branches(node))
            graph_children = set(graph.successors(node))
            self.assertEqual(db_children, graph_children, f"Children mismatch for node {node}")

    def test_branch_creation_verification_and_deletion(self):
        """
        A comprehensive test that creates a complex branch tree,
        verifies its structure, randomly deletes branches, and
        re-verifies the structure.
        """
        # Step 1: Generate the tree structure
        graph, roots = make_tree(1, 4, 0.8, 10)

        # Step 2: Add the graph to the database
        self._add_graph_to_db(graph, roots)

        # Step 3: Verify the initial structure
        self._verify_db_structure(graph)

        # Step 4: Randomly delete leaf nodes
        num_deletions = min(35, len(graph.nodes()) // 2)
        for _ in range(num_deletions):
            leaf_nodes = [node for node, out_degree in graph.out_degree() if out_degree == 0]
            if not leaf_nodes:
                break

            node_to_delete = random.choice(leaf_nodes)

            self.db.delete_branch(node_to_delete)
            graph.remove_node(node_to_delete)

        # Step 5: Verify the final structure
        self._verify_db_structure(graph)


if __name__ == '__main__':
    unittest.main()
