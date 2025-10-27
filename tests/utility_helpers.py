import numpy as np

def number_to_alpha_label(n):
    """
    Returns an alphabetic label for a number; 1 is a, 2 is b, 27 is aa, etc.
    """
    s = ''
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        s = chr(65 + remainder) + s
    return s.lower()

def name_tree(g, initial_node_name_prefix='', node_start=None):
    """
    Names the nodes in a tree structure with a given adjacency matrix.
    """
    if node_start is None:
        node_start = np.where(np.sum(g, axis=1) == 0)[0]

    node_names = [''] * g.shape[0]
    node_indexes = []

    for i, node_here in enumerate(node_start):
        prefix = initial_node_name_prefix
        if prefix and not prefix.endswith('_'):
            prefix += '_'

        node_names[node_here] = prefix + number_to_alpha_label(i + 1)
        node_indexes.append(node_here)

        next_nodes = np.where(g[:, node_here] == 1)[0]

        if next_nodes.any():
            sub_names, sub_indexes = name_tree(g, node_names[node_here], next_nodes)
            for k, index_here in enumerate(sub_indexes):
                if sub_names[index_here]:
                    if node_names[index_here]:
                        raise ValueError("We visited a node twice, should not happen in a real tree.")
                    node_names[index_here] = sub_names[index_here]
            node_indexes.extend(sub_indexes)

    return node_names, node_indexes

def make_tree(n_initial, children_rate, children_rate_decay, max_depth):
    """
    Constructs a random tree structure.
    """
    if max_depth < 0:
        children_rate = 0

    g = np.zeros((n_initial, n_initial))

    for i in range(n_initial):
        current_nodes = g.shape[0]
        num_children_here = np.random.poisson(children_rate)

        g_ = make_tree(num_children_here, children_rate * children_rate_decay, children_rate_decay, max_depth - 1)[0]

        new_g = np.zeros((g.shape[0] + g_.shape[0], g.shape[1] + g_.shape[1]))
        new_g[:g.shape[0], :g.shape[1]] = g
        new_g[g.shape[0]:, g.shape[1]:] = g_
        g = new_g

        if num_children_here > 0:
            g[current_nodes:current_nodes + num_children_here, i] = 1

    node_names = name_tree(g)[0]
    return g, node_names

def apply_did_query(docs, q):
    """
    Applies a did.query to a list of documents.
    """
    from did.datastructures.utils import field_search

    search_params = q.to_search_structure()
    ids = []
    d = []
    for doc in docs:
        if field_search(doc.document_properties, search_params):
            d.append(doc)
            ids.append(doc.id())
    return ids, d
