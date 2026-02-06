import abc

class Database(abc.ABC):
    def __init__(self, connection='', **kwargs):
        self.connection = connection
        self.version = None
        self.current_branch_id = ''
        self.frozen_branch_ids = []
        self.dbid = None
        self.preferences = {}
        self.debug = kwargs.get('debug', False)

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def open(self):
        return self._open_db()

    def close(self):
        self._close_db()

    @abc.abstractmethod
    def _open_db(self):
        pass

    @abc.abstractmethod
    def _close_db(self):
        pass

    def all_branch_ids(self):
        return self._do_get_branch_ids()

    def add_branch(self, branch_id, parent_branch_id=None):
        if parent_branch_id is None:
            parent_branch_id = self.current_branch_id

        # Validation logic would go here

        self._do_add_branch(branch_id, parent_branch_id)
        self.current_branch_id = branch_id

    def set_branch(self, branch_id):
        # Validation logic would go here
        self.current_branch_id = branch_id

    def get_branch(self):
        return self.current_branch_id

    # ... other branch-related methods would follow ...

    @abc.abstractmethod
    def _do_get_branch_ids(self):
        pass

    @abc.abstractmethod
    def _do_add_branch(self, branch_id, parent_branch_id):
        pass

    # ... other abstract do_* methods for branches ...

    def all_doc_ids(self):
        return self._do_get_doc_ids()

    def get_doc_ids(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        # Validation logic would go here
        return self._do_get_doc_ids(branch_id)

    def add_docs(self, document_objs, branch_id=None, **kwargs):
        if branch_id is None:
            branch_id = self.current_branch_id
        # Validation and other logic from the Matlab code would be ported here
        for doc in document_objs:
            self._do_add_doc(doc, branch_id, **kwargs)

    # ... other document-related methods would follow ...

    @abc.abstractmethod
    def _do_get_doc_ids(self, branch_id=None):
        pass

    @abc.abstractmethod
    def _do_add_doc(self, document_obj, branch_id, **kwargs):
        pass

    def get_docs(self, document_ids, branch_id=None, OnMissing='error', **kwargs):
        is_single = False
        if not isinstance(document_ids, list):
            document_ids = [document_ids]
            is_single = True

        # If branch_id is provided, we might want to validate it or pass it down.
        # Current _do_get_doc doesn't take branch_id, but maybe it should?
        # For now, I'll ignore passing it to _do_get_doc unless I change its signature.
        # But wait, checking if doc is in branch is important if branch_id is given.

        # Checking logic here (inefficient but generic):
        if branch_id is not None:
             branch_doc_ids = self.get_doc_ids(branch_id)
             # If branch doesn't exist? get_doc_ids might return empty or raise?
             # get_doc_ids calls _do_get_doc_ids.

        docs = []
        for doc_id in document_ids:
            if branch_id is not None:
                if doc_id not in branch_doc_ids:
                    # Document not in branch
                    if OnMissing == 'error':
                        raise ValueError(f"Document {doc_id} not found in branch {branch_id}")
                    elif OnMissing == 'warn':
                         print(f"Warning: Document {doc_id} not found in branch {branch_id}")
                         continue
                    else:
                         continue

            docs.append(self._do_get_doc(doc_id, OnMissing=OnMissing, **kwargs))

        if not docs and OnMissing != 'ignore' and len(document_ids) > 0:
             # If filtered out all?
             pass

        if is_single:
             return docs[0] if docs else None
        else:
             return docs

    @abc.abstractmethod
    def _do_get_doc(self, document_id, OnMissing='error', **kwargs):
        pass

    def remove_docs(self, document_ids, branch_id=None, **kwargs):
        if not isinstance(document_ids, list):
            document_ids = [document_ids]

        if branch_id is None:
            branch_id = self.current_branch_id

        for doc_id in document_ids:
            self._do_remove_doc(doc_id, branch_id, **kwargs)

    @abc.abstractmethod
    def _do_remove_doc(self, document_id, branch_id, **kwargs):
        pass

    def delete_branch(self, branch_id):
        # Validation logic would go here
        self._do_delete_branch(branch_id)

    @abc.abstractmethod
    def _do_delete_branch(self, branch_id):
        pass

    def get_sub_branches(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        return self._do_get_sub_branches(branch_id)

    @abc.abstractmethod
    def _do_get_sub_branches(self, branch_id):
        pass

    def get_branch_parent(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        return self._do_get_branch_parent(branch_id)

    @abc.abstractmethod
    def _do_get_branch_parent(self, branch_id):
        pass

    def search(self, query_obj, branch_id=None):
        from .datastructures import field_search

        if branch_id is None:
            branch_id = self.current_branch_id

        doc_ids = self.get_doc_ids(branch_id)
        docs = self.get_docs(doc_ids, OnMissing='ignore')
        if docs is None: docs = []
        if not isinstance(docs, list): docs = [docs]

        search_params = query_obj.to_search_structure()

        matched_ids = []
        for doc in docs:
            if doc and field_search(doc.document_properties, search_params):
                matched_ids.append(doc.id())

        return matched_ids

    # ... other abstract do_* methods for documents ...

    @abc.abstractmethod
    def do_run_sql_query(self, query_str, **kwargs):
        pass