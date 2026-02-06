# `did.database`

The `did.database` module provides the core `Database` class, which is an abstract base class that defines the API for interacting with a DID database.

## `Database` Class

The `Database` class is an abstract base class that defines the interface for all database implementations. It provides methods for managing branches, adding and retrieving documents, and performing searches.

### Key Methods

*   `add_branch(branch_id, parent_branch_id=None)`: Adds a new branch to the database.
*   `get_branch()`: Returns the current branch ID.
*   `set_branch(branch_id)`: Sets the current branch.
*   `all_branch_ids()`: Returns a list of all branch IDs.
*   `add_docs(document_objs, branch_id=None, **kwargs)`: Adds documents to the database.
*   `get_docs(document_ids, **kwargs)`: Retrieves documents from the database.
*   `remove_docs(document_ids, branch_id=None, **kwargs)`: Removes documents from the database.
*   `search(query)`: Searches the database using a `did.query.Query` object.

### Abstract Methods

The `Database` class has several abstract methods that must be implemented by subclasses. These methods, which are prefixed with `_do_`, handle the low-level details of interacting with the database.

*   `_do_add_branch(branch_id, parent_branch_id)`
*   `_do_get_branch_ids()`
*   `_do_add_doc(document_obj, branch_id, **kwargs)`
*   `_do_get_doc(document_id, **kwargs)`
*   `_do_remove_doc(document_id, branch_id, **kwargs)`
*   `_do_run_sql_query(query_str, **kwargs)`