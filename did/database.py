from abc import ABC, abstractmethod
import re
from .datastructures.utils import field_search

class Database(ABC):
    """
    Abstract superclass for all did.database implementations.
    """

    def __init__(self, connection=''):
        self.connection = connection
        self.version = None
        self.current_branch_id = ''
        self.frozen_branch_ids = []
        self.dbid = None
        self.preferences = {}
        self.debug = False

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def open(self):
        self.open_db()

    def close(self):
        self.close_db()

    def all_branch_ids(self):
        return self.do_get_branch_ids()

    def add_branch(self, branch_id, parent_branch_id=None):
        if parent_branch_id is None:
            parent_branch_id = self.current_branch_id

        branch_id, branch_ids = self._validate_branch_id(branch_id, check_existence=False)
        if branch_id in branch_ids:
            raise ValueError(f"Branch id '{branch_id}' already exists in the database.")
        if parent_branch_id and parent_branch_id not in branch_ids:
            raise ValueError(f"Parent branch id '{parent_branch_id}' does not exist in the database.")

        if parent_branch_id:
            parent_branch_id = self._validate_branch_id(parent_branch_id)[0]

        self.do_add_branch(branch_id, parent_branch_id)
        self.current_branch_id = branch_id

    def set_branch(self, branch_id):
        branch_id, _ = self._validate_branch_id(branch_id)
        self.current_branch_id = branch_id

    def get_branch(self):
        return self.current_branch_id

    def get_branch_parent(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        return self.do_get_branch_parent(branch_id)

    def get_sub_branches(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        return self.do_get_sub_branches(branch_id)

    def freeze_branch(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        if branch_id not in self.frozen_branch_ids:
            self.frozen_branch_ids.append(branch_id)

    def is_branch_editable(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        return branch_id not in self.frozen_branch_ids and not self.do_get_sub_branches(branch_id)

    def delete_branch(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id

        branch_id, branch_ids = self._validate_branch_id(branch_id)
        if branch_id in self.frozen_branch_ids:
            raise ValueError(f"Branch id '{branch_id}' is frozen and cannot be deleted.")
        if self.get_sub_branches(branch_id):
            raise ValueError(f"Branch id '{branch_id}' has sub-branches and cannot be deleted.")

        self.do_delete_branch(branch_id)
        if branch_id in self.frozen_branch_ids:
            self.frozen_branch_ids.remove(branch_id)

        if self.current_branch_id == branch_id:
            self.current_branch_id = branch_ids[0] if branch_ids and branch_ids[0] != branch_id else ''

    def display_branches(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)

        def _display_sub_branches(branch_id, indent):
            print(f"{'  ' * indent} - {branch_id}")
            for sub_id in self.get_sub_branches(branch_id):
                _display_sub_branches(sub_id, indent + 1)

        _display_sub_branches(branch_id, 0)

    def all_doc_ids(self):
        return self.do_get_doc_ids()

    def get_doc_ids(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        return self.do_get_doc_ids(branch_id)

    def add_docs(self, document_objs, branch_id=None, on_duplicate='error', validate=True):
        if not document_objs:
            return

        if not isinstance(document_objs, list):
            document_objs = [document_objs]

        if branch_id is None:
            branch_id = self.current_branch_id

        self.set_branch(branch_id)

        self.open()

        if validate:
            self._validate_docs(document_objs)

        for doc in document_objs:
            if self.debug:
                try:
                    doc_props = doc.document_properties
                    doc_id = doc_props.get('base', {}).get('id', '')
                    class_name = doc_props.get('document_class', {}).get('class_name', '<unknown class>')
                    print(f"Adding {class_name} doc {doc_id} to database branch {branch_id}")
                except:
                    pass
            self.do_add_doc(doc, branch_id, on_duplicate=on_duplicate)

    def get_docs(self, document_ids=None, branch_id=None, on_missing='error'):
        self.open()

        if document_ids is None:
            document_ids = self.get_doc_ids(branch_id=branch_id)

        if not document_ids:
            return []

        if not isinstance(document_ids, list):
            document_ids = [document_ids]

        document_objs = []
        for doc_id in document_ids:
            doc_id = self._validate_doc_id(doc_id, check_existence=False)
            document_objs.append(self.do_get_doc(doc_id, on_missing=on_missing))

        return document_objs

    def remove_docs(self, documents, branch_id=None, on_missing='error'):
        if not documents:
            return

        if not isinstance(documents, list):
            documents = [documents]

        if branch_id is None:
            branch_id = self.current_branch_id

        branch_id, _ = self._validate_branch_id(branch_id)

        for doc in documents:
            doc_id = self._validate_doc_id(doc, check_existence=False)
            try:
                self.do_remove_doc(doc_id, branch_id, on_missing=on_missing)
            except:
                pass

    def open_doc(self, document_id, filename, **options):
        document_id = self._validate_doc_id(document_id, check_existence=False)
        return self.do_open_doc(document_id, filename, **options)

    def exist_doc(self, document_id, filename, **options):
        document_id = self._validate_doc_id(document_id, check_existence=False)
        return self.check_exist_doc(document_id, filename, **options)

    def close_doc(self, file_obj):
        self.do_close_doc(file_obj)

    def run_sql_query(self, query_str, return_struct=False):
        data = self.do_run_sql_query(query_str)
        if not data or return_struct:
            return data

        if isinstance(data, list) and isinstance(data[0], dict):
            return [[item[key] for item in data] for key in data[0]]
        return data

    def search(self, query_obj, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        return self.do_search(query_obj, branch_id)

    def _validate_branch_id(self, branch_id, check_existence=True):
        if not isinstance(branch_id, str) or not branch_id:
            raise ValueError("Branch ID must be a non-empty string.")

        branch_ids = self.all_branch_ids()
        if check_existence and branch_id not in branch_ids:
            raise ValueError(f"Branch ID '{branch_id}' does not exist in the database.")

        return branch_id, branch_ids

    def _validate_doc_id(self, doc_id, check_existence=True):
        if hasattr(doc_id, 'id'):
            doc_id = doc_id.id()
        elif isinstance(doc_id, dict):
            try:
                doc_id = doc_id['document_properties']['ndi_document']['id']
            except KeyError:
                raise ValueError("Input document must be a valid document object or ID.")
        elif not isinstance(doc_id, str) or not doc_id:
            raise ValueError("Input document must be a valid document object or ID.")

        if check_existence:
            doc_ids = self.get_doc_ids()
            if doc_id not in doc_ids:
                raise ValueError(f"Document ID '{doc_id}' does not exist in the database.")

        return doc_id

    def _validate_docs(self, document_objs):
        # This method is a placeholder for the complex validation logic.
        pass

    def get_preference_names(self):
        return list(self.preferences.keys())

    def get_preference(self, pref_name, default_value=None):
        if not isinstance(pref_name, str) or not pref_name:
            raise ValueError("The get_preference method requires a valid preference name input parameter.")

        return self.preferences.get(pref_name, default_value)

    def set_preference(self, pref_name, value=None):
        if not isinstance(pref_name, str) or not pref_name:
            raise ValueError("The set_preference method requires a valid preference name input parameter.")

        self.preferences[pref_name] = value

    @abstractmethod
    def do_run_sql_query(self, query_str, *args, **kwargs):
        pass

    @abstractmethod
    def do_get_branch_ids(self):
        pass

    @abstractmethod
    def do_add_branch(self, branch_id, parent_branch_id, *args, **kwargs):
        pass

    @abstractmethod
    def do_delete_branch(self, branch_id, *args, **kwargs):
        pass

    @abstractmethod
    def do_get_branch_parent(self, branch_id, *args, **kwargs):
        pass

    @abstractmethod
    def do_get_sub_branches(self, branch_id, *args, **kwargs):
        pass

    @abstractmethod
    def do_get_doc_ids(self, branch_id=None, *args, **kwargs):
        pass

    @abstractmethod
    def do_add_doc(self, document_obj, branch_id, *args, **kwargs):
        pass

    @abstractmethod
    def do_get_doc(self, document_id, *args, **kwargs):
        pass

    @abstractmethod
    def do_remove_doc(self, document_id, branch_id, *args, **kwargs):
        pass

    @abstractmethod
    def do_open_doc(self, document_id, filename, *args, **kwargs):
        pass

    @abstractmethod
    def check_exist_doc(self, document_id, filename, *args, **kwargs):
        pass

    def open_db(self):
        pass

    def close_db(self):
        pass

    def do_search(self, query_obj, branch_id):
        # This method can be overridden by subclasses if a more efficient
        # search mechanism is available.
        if hasattr(query_obj, 'to_search_structure'):
            query_obj = query_obj.to_search_structure()

        # This is where the conversion from search_structure to SQL would happen.
        # For now, we will just use fieldsearch for a simplified implementation.
        all_docs = self.get_docs(branch_id=branch_id)
        matching_ids = []
        for doc in all_docs:
            if field_search(doc.document_properties, query_obj):
                matching_ids.append(doc.id())
        return matching_ids
