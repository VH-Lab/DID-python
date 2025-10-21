import random
import numpy as np
import networkx as nx
from src.did.document import Document

def make_doc_tree(rates):
    """
    Makes a 'tree' of documents to add to a database.
    """
    num_a = np.random.poisson(rates[0])
    num_b = np.random.poisson(rates[1])
    num_c = np.random.poisson(rates[2])

    g = nx.DiGraph()
    docs = []
    node_names = []
    ids_a = []
    ids_b = []
    ids_c = []

    counter = 1

    for _ in range(num_a):
        doc = Document('demoA', **{'demoA.value': counter})
        docs.append(doc)
        node_names.append(str(counter))
        ids_a.append(doc.id())
        g.add_node(doc.id())
        counter += 1

    for _ in range(num_b):
        doc = Document('demoB', **{'demoB.value': counter, 'demoA.value': counter})
        docs.append(doc)
        node_names.append(str(counter))
        ids_b.append(doc.id())
        g.add_node(doc.id())
        counter += 1

    c_count = 0
    for _ in range(num_c):
        doc = Document('demoC', **{'demoC.value': counter})
        docs.append(doc)
        node_names.append(str(counter))
        ids_c.append(doc.id())
        g.add_node(doc.id())

        dep_a = random.randint(0, num_a - 1) if num_a > 0 else -1
        dep_b = random.randint(0, num_b - 1) if num_b > 0 else -1
        dep_c = random.randint(0, c_count - 1) if c_count > 0 else -1

        if dep_a >= 0:
            doc.set_dependency_value('item1', ids_a[dep_a], error_if_not_found=False)
            g.add_edge(ids_a[dep_a], doc.id())
        if dep_b >= 0:
            doc.set_dependency_value('item2', ids_b[dep_b], error_if_not_found=False)
            g.add_edge(ids_b[dep_b], doc.id())
        if dep_c >= 0:
            doc.set_dependency_value('item3', ids_c[dep_c], error_if_not_found=False)
            g.add_edge(ids_c[dep_c], doc.id())

        if 'depends_on' not in doc.document_properties:
            doc.document_properties['depends_on'] = []

        counter += 1
        c_count += 1

    return g, node_names, docs

def verify_db_document_structure(db, g, expected_docs, OnMissing='error'):
    """
    Verifies that the documents in the database match the expected documents and their relationships.
    """
    from src.did.datastructures import eq_len

    fieldset = ['demoA', 'demoB', 'demoC']

    for doc in expected_docs:
        id_here = doc.id()
        doc_here = db.get_docs(id_here)

        if not doc_here:
            return False, f"Document with id {id_here} not found in the database."

        # Test whether the content matches
        for field in fieldset:
            if field in doc.document_properties:
                if field in doc_here.document_properties:
                    field1 = doc.document_properties[field]
                    field2 = doc_here.document_properties[field]
                    if not eq_len(field1, field2):
                        return False, f"Field {field} of document {id_here} did not match."
                else:
                    return False, f"Field {field} not found in document {id_here} from the database."

    errors = []

    fieldset = ['demoA', 'demoB', 'demoC']

    for doc in expected_docs:
        id_here = doc.id()
        try:
            doc_here = db.get_docs(id_here, OnMissing=OnMissing)
        except ValueError:
            doc_here = None

        if doc_here is None:
            if OnMissing.lower() == 'ignore':
                continue
            else:
                errors.append(f"Document with id {id_here} not found in the database.")
                continue

        # Test whether the content matches
        for field in fieldset:
            if field in doc.document_properties:
                if field in doc_here.document_properties:
                    field1 = doc.document_properties[field]
                    field2 = doc_here.document_properties[field]
                    if not eq_len(field1, field2):
                        errors.append(f"Field {field} of document {id_here} did not match.")
                else:
                    errors.append(f"Field {field} not found in document {id_here} from the database.")

    return not errors, "\n".join(errors)

def number_to_alpha_label(n):
    """
    Converts a number to an alphabetic label.
    """
    s = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        s = chr(65 + remainder) + s
    return s.lower()

def name_tree(g, initial_node_name_prefix="", node_start=None):
    """
    Names the nodes in a tree structure.
    """
    node_names = {n: "" for n in g.nodes()}

    if node_start is None:
        starting_nodes = [n for n, d in g.in_degree() if d == 0]
    else:
        starting_nodes = node_start if isinstance(node_start, list) else [node_start]

    for i, node in enumerate(starting_nodes):
        label = number_to_alpha_label(i + 1)
        node_names[node] = f"{initial_node_name_prefix}{label}"

        # Recursive naming of children
        q = [(node, node_names[node])]
        visited = {node}
        while q:
            parent, parent_name = q.pop(0)
            children = list(g.successors(parent))
            for j, child in enumerate(children):
                if child not in visited:
                    child_label = number_to_alpha_label(j + 1)
                    node_names[child] = f"{parent_name}_{child_label}"
                    visited.add(child)
                    q.append((child, node_names[child]))

    return node_names

def make_tree(n_initial, children_rate, children_rate_decay, max_depth):
    """
    Constructs a random tree structure.
    """
    if max_depth < 0:
        return nx.DiGraph()

    g = nx.DiGraph()
    nodes = list(range(n_initial))
    g.add_nodes_from(nodes)

    for i in range(n_initial):
        current_node_count = len(g.nodes())
        num_children_here = np.random.poisson(children_rate)

        if num_children_here > 0:
            sub_g = make_tree(num_children_here, children_rate * children_rate_decay, children_rate_decay, max_depth - 1)

            # Renumber nodes in sub_g to avoid conflicts
            mapping = {n: n + current_node_count for n in sub_g.nodes()}
            sub_g = nx.relabel_nodes(sub_g, mapping)

            g.add_nodes_from(sub_g.nodes())
            g.add_edges_from(sub_g.edges())

            # Connect new children to the current node
            for j in range(num_children_here):
                g.add_edge(nodes[i], current_node_count + j)

    return g

def add_branch_nodes(db, starting_db_branch_id, g, node_names, node_start_index=None):
    """
    Adds a tree of nodes to a DID database.
    """
    if node_start_index is None:
        node_start_index = [n for n, d in g.in_degree() if d == 0]

    q = [(starting_db_branch_id, n) for n in node_start_index]
    visited = set()

    while q:
        parent_branch, node = q.pop(0)
        if node in visited:
            continue
        visited.add(node)

        node_name = node_names[node]

        db.add_branch(node_name, parent_branch_id=parent_branch)

        children = list(g.successors(node))
        for child in children:
            q.append((node_name, child))

def verify_branch_nodes(db, g, node_names):
    """
    Verifies all branch nodes in a digraph are in the database.
    """
    all_branches = db.all_branch_ids()
    missing = set(node_names.values()) - set(all_branches)
    return not missing, list(missing)

def verify_branch_node_structure(db, g, node_names):
    """
    Verifies branch structure in a digraph are in the database.
    """
    current_branch = db.get_branch()

    for node, node_name in node_names.items():
        # Get expected parents and children from the graph
        expected_parents = [node_names[p] for p in g.predecessors(node)] if node in g else []
        expected_children = [node_names[c] for c in g.successors(node)] if node in g else []

        # Get actual parents and children from the database
        actual_parent = db.get_branch_parent(node_name)
        actual_children = db.get_sub_branches(node_name)

        if not actual_parent:
            actual_parent = []
        else:
            actual_parent = [actual_parent]

        # Compare parents
        if set(expected_parents) != set(actual_parent):
            return False, f"Error in parent of {node_name}. Expected {expected_parents}, got {actual_parent}"

        # Compare children
        if set(expected_children) != set(actual_children):
            return False, f"Error in sub_branch of {node_name}. Expected {expected_children}, got {actual_children}"

    db.set_branch(current_branch)
    return True, ""

def delete_random_branch(db, g, node_names):
    """
    Deletes a random branch from a database and digraph.
    """
    leaf_nodes = [n for n, d in g.out_degree() if d == 0]
    if not leaf_nodes:
        return g, node_names

    node_to_remove = random.choice(leaf_nodes)
    node_name_to_remove = node_names[node_to_remove]

    db.delete_branch(node_name_to_remove)
    g.remove_node(node_to_remove)
    del node_names[node_to_remove]

    return g, node_names

def get_demo_type(doc):
    """
    Finds the first demo class for a given document.
    """
    if 'demoA' in doc.document_properties:
        return 'demoA'
    elif 'demoB' in doc.document_properties:
        return 'demoB'
    elif 'demoC' in doc.document_properties:
        return 'demoC'
    return ''

def apply_did_query(docs, q):
    """
    Returns the expected document and id output of the selected query.
    """
    from src.did.datastructures import field_search

    search_params = q.to_search_structure()
    ids = []
    d = []
    for doc in docs:
        if field_search(doc.document_properties, search_params):
            d.append(doc)
            ids.append(doc.id())
    return ids, d