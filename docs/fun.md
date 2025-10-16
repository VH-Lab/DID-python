# `did.fun`

The `did.fun` module provides functions for analyzing and visualizing the relationships between documents in the database.

## Key Functions

*   `docs_to_graph(document_objs)`: Creates a `networkx.DiGraph` object from a list of `Document` objects. The nodes of the graph are the document IDs, and the edges represent the dependencies between the documents.

*   `find_all_dependencies(graph, doc_ids)`: Finds all documents that depend on a given set of documents. This function takes a `networkx.DiGraph` object and a list of document IDs and returns a list of all the documents that depend on them.

*   `find_docs_missing_dependencies(db, *dependency_names)`: Finds documents that have dependencies on documents that do not exist in the database.

*   `plot_interactive_doc_graph(docs, g, layout='spring')`: Plots an interactive graph of the documents and their dependencies. Clicking on a node in the plot will display information about the corresponding document.