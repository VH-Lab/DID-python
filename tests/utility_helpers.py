import networkx as nx
import numpy as np
from did.datastructures.utils import field_search

def number_to_alpha_label(n):
    """
    Converts a positive integer to a base-26 alphabetic label.
    1 -> 'a', 2 -> 'b', ..., 26 -> 'z', 27 -> 'aa', etc.
    """
    if n <= 0:
        raise ValueError("Input must be a positive integer")

    label = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        label = chr(97 + remainder) + label
    return label

def make_tree(n_initial, children_rate, children_rate_decay, max_depth):
    """
    Generates a random tree structure using networkx.
    Returns a networkx.DiGraph and a list of the root node names.
    """
    graph = nx.DiGraph()

    root_nodes = [number_to_alpha_label(i + 1) for i in range(n_initial)]
    for node_name in root_nodes:
        graph.add_node(node_name)

    q = [(root_name, 0) for root_name in root_nodes]

    head = 0
    while head < len(q):
        parent_name, depth = q[head]
        head += 1

        if depth >= max_depth:
            continue

        current_rate = children_rate * (children_rate_decay ** depth)
        num_children = np.random.poisson(current_rate)

        for i in range(num_children):
            child_suffix = number_to_alpha_label(i + 1)
            child_name = f"{parent_name}_{child_suffix}"
            graph.add_node(child_name)
            graph.add_edge(parent_name, child_name)
            q.append((child_name, depth + 1))

    return graph, root_nodes

def apply_did_query(docs, q):
    """
    Applies a did.query to a list of documents and returns the matching
    documents and their IDs.
    """
    search_params = q.to_search_structure()
    matching_docs = []
    matching_ids = []
    for doc in docs:
        if field_search(doc.document_properties, search_params):
            matching_docs.append(doc)
            matching_ids.append(doc.id())
    return matching_ids, matching_docs
