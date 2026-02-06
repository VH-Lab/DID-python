import networkx as nx

def docs_to_graph(document_objs):
    """
    Creates a directed graph from a list of Document objects.

    This function mimics the behavior of the Matlab `docs2graph` function.
    """
    g = nx.DiGraph()
    nodes = [doc.id() for doc in document_objs]
    g.add_nodes_from(nodes)

    for doc in document_objs:
        here_node = doc.id()
        dependencies = doc.document_properties.get('depends_on', [])
        for dep in dependencies:
            there_node = dep.get('value')
            if there_node in nodes:
                # Edge from B to A if A depends on B
                g.add_edge(there_node, here_node)

    # In networkx, the graph object itself is the primary output.
    # The adjacency matrix and node list can be accessed from the graph object.
    return g

def find_all_dependencies(graph, doc_ids):
    """
    Finds all documents that depend on a given set of documents.
    """
    all_deps = set()
    for doc_id in doc_ids:
        if graph.has_node(doc_id):
            all_deps.update(nx.descendants(graph, doc_id))
    return list(all_deps)

def find_docs_missing_dependencies(db, *dependency_names):
    """
    Finds documents that have dependencies on documents that do not exist.
    """
    from .query import Query

    q = Query('depends_on', 'hasfield', '', '')
    docs_with_deps = db.search(q)

    missing_deps_docs = []

    all_doc_ids = db.all_doc_ids()

    for doc in docs_with_deps:
        dependencies = doc.document_properties.get('depends_on', [])
        for dep in dependencies:
            dep_name = dep.get('name')
            dep_value = dep.get('value')

            if dependency_names and dep_name not in dependency_names:
                continue

            if dep_value and dep_value not in all_doc_ids:
                missing_deps_docs.append(doc)
                break # Move to the next document

    return missing_deps_docs

def plot_interactive_doc_graph(docs, g, layout='spring'):
    """
    Plots an interactive document graph.

    This function mimics the behavior of the Matlab `plotinteractivedocgraph` function.
    """
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()

    if layout == 'layered':
        pos = nx.nx_agraph.graphviz_layout(g, prog='dot')
    else:
        pos = nx.spring_layout(g)

    nx.draw(g, pos, with_labels=True, ax=ax)

    def on_click(event):
        if event.inaxes is None:
            return

        # Find the closest node to the click
        min_dist = float('inf')
        closest_node = None
        for node, (x, y) in pos.items():
            dist = (x - event.xdata)**2 + (y - event.ydata)**2
            if dist < min_dist:
                min_dist = dist
                closest_node = node

        if closest_node:
            # Find the corresponding document
            clicked_doc = None
            for doc in docs:
                if doc.id() == closest_node:
                    clicked_doc = doc
                    break

            if clicked_doc:
                print(f"Clicked node: {closest_node}")
                print("Document properties:")
                # A more sophisticated display could be implemented here
                print(clicked_doc.document_properties)

                # Set a global variable (as in the Matlab code)
                # Note: Using global variables is generally discouraged in Python,
                # but it's done here to mimic the original functionality.
                global clicked_node
                clicked_node = clicked_doc
                print("Global variable 'clicked_node' set to clicked document")

    fig.canvas.mpl_connect('button_press_event', on_click)
    plt.show()