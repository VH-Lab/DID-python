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

    def get_docs(self, document_ids, OnMissing='error'):
        if not isinstance(document_ids, list):
            document_ids = [document_ids]

        docs = []
        for doc_id in document_ids:
            docs.append(self._do_get_doc(doc_id, OnMissing=OnMissing))

        return docs[0] if len(docs) == 1 else docs

    @abc.abstractmethod
    def _do_get_doc(self, document_id, OnMissing='error'):
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

    # ... other abstract do_* methods for documents ...

    @abc.abstractmethod
    def do_run_sql_query(self, query_str, **kwargs):
        pass