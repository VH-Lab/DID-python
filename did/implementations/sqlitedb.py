import sqlite3
import json
import os
import shutil
from ..database import Database
from ..document import Document
from ..datastructures.utils import json_encode_nan
from ..common.path_constants import PathConstants

class SQLiteDB(Database):
    def __init__(self, filename):
        super().__init__(filename)
        self.file_dir = os.path.join(os.path.dirname(filename), 'files')
        os.makedirs(self.file_dir, exist_ok=True)
        self.open_db()

    def open_db(self):
        self.conn = sqlite3.connect(self.connection)
        self.conn.row_factory = sqlite3.Row
        self._create_db_tables()

    def close_db(self):
        if self.conn:
            self.conn.close()

    def _create_db_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS branches (
                branch_id TEXT NOT NULL UNIQUE,
                parent_id TEXT,
                timestamp REAL,
                PRIMARY KEY(branch_id),
                FOREIGN KEY(parent_id) REFERENCES branches(branch_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS docs (
                doc_id TEXT NOT NULL UNIQUE,
                doc_idx INTEGER NOT NULL UNIQUE,
                json_code TEXT,
                timestamp REAL,
                PRIMARY KEY(doc_idx AUTOINCREMENT)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS branch_docs (
                branch_id TEXT NOT NULL,
                doc_idx INTEGER NOT NULL,
                timestamp REAL,
                FOREIGN KEY(branch_id) REFERENCES branches(branch_id),
                FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx),
                PRIMARY KEY(branch_id, doc_idx)
            )
        ''')
        self.conn.commit()

    def do_run_sql_query(self, query_str, *args):
        cursor = self.conn.cursor()
        cursor.execute(query_str, args)
        return [dict(row) for row in cursor.fetchall()]

    def do_get_branch_ids(self):
        data = self.do_run_sql_query('SELECT DISTINCT branch_id FROM branches')
        return [row['branch_id'] for row in data]

    def do_add_branch(self, branch_id, parent_branch_id):
        cursor = self.conn.cursor()
        cursor.execute('INSERT INTO branches (branch_id, parent_id, timestamp) VALUES (?, ?, ?)',
                       (branch_id, parent_branch_id, 0))
        self.conn.commit()
        if parent_branch_id:
            parent_docs = self.do_run_sql_query('SELECT doc_idx FROM branch_docs WHERE branch_id = ?', parent_branch_id)
            for doc in parent_docs:
                self.do_run_sql_query('INSERT INTO branch_docs (branch_id, doc_idx, timestamp) VALUES (?, ?, ?)',
                                      branch_id, doc['doc_idx'], 0)

    def do_delete_branch(self, branch_id):
        self.do_run_sql_query('DELETE FROM branch_docs WHERE branch_id = ?', branch_id)
        self.do_run_sql_query('DELETE FROM branches WHERE branch_id = ?', branch_id)
        self.conn.commit()

    def do_get_branch_parent(self, branch_id):
        data = self.do_run_sql_query('SELECT parent_id FROM branches WHERE branch_id = ?', branch_id)
        return data[0]['parent_id'] if data else None

    def do_get_sub_branches(self, branch_id):
        data = self.do_run_sql_query('SELECT branch_id FROM branches WHERE parent_id = ?', branch_id)
        return [row['branch_id'] for row in data]

    def do_get_doc_ids(self, branch_id=None):
        if branch_id:
            sql = '''
                SELECT d.doc_id FROM docs d
                JOIN branch_docs bd ON d.doc_idx = bd.doc_idx
                WHERE bd.branch_id = ?
            '''
            params = (branch_id,)
        else:
            sql = 'SELECT doc_id FROM docs'
            params = ()
        data = self.do_run_sql_query(sql, *params)
        return [row['doc_id'] for row in data]

    def do_add_doc(self, document_obj, branch_id, on_duplicate='error'):
        doc_id = document_obj.id()

        # File ingestion logic
        if 'files' in document_obj.document_properties and 'file_info' in document_obj.document_properties['files']:
            for file_info in document_obj.document_properties['files']['file_info']:
                for location in file_info['locations']:
                    if location.get('ingest'):
                        src = location['location']
                        dst = os.path.join(self.file_dir, location['uid'])
                        if os.path.exists(src):
                            shutil.copy(src, dst)
                            if location.get('delete_original'):
                                os.remove(src)

        existing_doc = self.do_run_sql_query('SELECT doc_idx FROM docs WHERE doc_id = ?', doc_id)

        if not existing_doc:
            json_code = json_encode_nan(document_obj.document_properties)
            cursor = self.conn.cursor()
            cursor.execute('INSERT INTO docs (doc_id, json_code, timestamp) VALUES (?, ?, ?)',
                           (doc_id, json_code, 0))
            self.conn.commit()
            doc_idx = cursor.lastrowid
        else:
            doc_idx = existing_doc[0]['doc_idx']

        existing_branch_doc = self.do_run_sql_query('SELECT doc_idx FROM branch_docs WHERE doc_idx = ? AND branch_id = ?', doc_idx, branch_id)
        if existing_branch_doc:
            if on_duplicate == 'error':
                raise ValueError(f"Document {doc_id} already exists in branch {branch_id}")
            elif on_duplicate == 'warn':
                print(f"Warning: Document {doc_id} already exists in branch {branch_id}")
        else:
            self.do_run_sql_query('INSERT INTO branch_docs (branch_id, doc_idx, timestamp) VALUES (?, ?, ?)', branch_id, doc_idx, 0)
            self.conn.commit()

    def do_get_doc(self, document_id, on_missing='error'):
        data = self.do_run_sql_query('SELECT json_code FROM docs WHERE doc_id = ?', document_id)
        if not data:
            if on_missing == 'error':
                raise ValueError(f"Document id '{document_id}' not found.")
            elif on_missing == 'warn':
                print(f"Warning: Document id '{document_id}' not found.")
            return None

        doc_struct = json.loads(data[0]['json_code'])
        return Document(doc_struct)

    def do_remove_doc(self, document_id, branch_id, on_missing='error'):
        doc = self.do_run_sql_query('SELECT doc_idx FROM docs WHERE doc_id = ?', document_id)
        if not doc:
            if on_missing == 'error':
                raise ValueError(f"Document id '{document_id}' not found.")
            elif on_missing == 'warn':
                print(f"Warning: Document id '{document_id}' not found.")
            return

        doc_idx = doc[0]['doc_idx']
        self.do_run_sql_query('DELETE FROM branch_docs WHERE doc_idx = ? AND branch_id = ?', doc_idx, branch_id)
        self.conn.commit()

    def do_open_doc(self, document_id, filename, **options):
        # This is a simplified implementation that assumes the file is in the file_dir.
        # A more complete implementation would handle URLs and other location types.
        doc = self.do_get_doc(document_id)
        if 'files' in doc.document_properties and 'file_info' in doc.document_properties['files']:
            for file_info in doc.document_properties['files']['file_info']:
                if file_info['name'] == filename:
                    for location in file_info['locations']:
                        if location['location_type'] == 'file':
                            path = os.path.join(self.file_dir, location['uid'])
                            if os.path.exists(path):
                                # This should return a file-like object
                                return open(path, 'rb')
        return None

    def check_exist_doc(self, document_id, filename, **options):
        doc = self.do_get_doc(document_id)
        if 'files' in doc.document_properties and 'file_info' in doc.document_properties['files']:
            for file_info in doc.document_properties['files']['file_info']:
                if file_info['name'] == filename:
                    for location in file_info['locations']:
                        if location['location_type'] == 'file':
                            path = os.path.join(self.file_dir, location['uid'])
                            return os.path.exists(path), path
        return False, ''
