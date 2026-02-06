import sqlite3
import os
from ..database import Database

class SQLiteDB(Database):
    def __init__(self, filename):
        super().__init__(connection=filename)
        self.dbid = None
        self._open_db()

    def _open_db(self):
        if self.dbid:
            return

        is_new = not os.path.exists(self.connection)
        self.dbid = sqlite3.connect(self.connection)
        self.dbid.execute("PRAGMA foreign_keys = ON")
        self.dbid.row_factory = sqlite3.Row

        if is_new:
            self._create_db_tables()

    def _close_db(self):
        if self.dbid:
            self.dbid.close()
            self.dbid = None

    def _create_db_tables(self):
        cursor = self.dbid.cursor()

        # Create branches table
        cursor.execute('''
            CREATE TABLE branches (
                branch_id TEXT NOT NULL UNIQUE,
                parent_id TEXT,
                timestamp REAL,
                FOREIGN KEY(parent_id) REFERENCES branches(branch_id),
                PRIMARY KEY(branch_id)
            )
        ''')

        # Create docs table
        cursor.execute('''
            CREATE TABLE docs (
                doc_id TEXT NOT NULL UNIQUE,
                doc_idx INTEGER NOT NULL UNIQUE,
                json_code TEXT,
                timestamp REAL,
                PRIMARY KEY(doc_idx AUTOINCREMENT)
            )
        ''')

        # Create branch_docs table
        cursor.execute('''
            CREATE TABLE branch_docs (
                branch_id TEXT NOT NULL,
                doc_idx INTEGER NOT NULL,
                timestamp REAL,
                FOREIGN KEY(branch_id) REFERENCES branches(branch_id),
                FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx),
                PRIMARY KEY(branch_id, doc_idx)
            )
        ''')

        # Create fields table
        cursor.execute('''
            CREATE TABLE fields (
                class TEXT NOT NULL,
                field_name TEXT NOT NULL UNIQUE,
                json_name TEXT NOT NULL,
                field_idx INTEGER NOT NULL UNIQUE,
                PRIMARY KEY(field_idx AUTOINCREMENT)
            )
        ''')

        # Create doc_data table
        cursor.execute('''
            CREATE TABLE doc_data (
                doc_idx INTEGER NOT NULL,
                field_idx INTEGER NOT NULL,
                value BLOB,
                FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx),
                FOREIGN KEY(field_idx) REFERENCES fields(field_idx)
            )
        ''')

        # Create files table
        cursor.execute('''
            CREATE TABLE files (
                doc_idx INTEGER NOT NULL,
                filename TEXT NOT NULL,
                uid TEXT NOT NULL UNIQUE,
                orig_location TEXT NOT NULL,
                cached_location TEXT,
                type TEXT NOT NULL,
                parameters TEXT,
                FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx),
                PRIMARY KEY(doc_idx, filename, uid)
            )
        ''')

        self.dbid.commit()

    def do_run_sql_query(self, query_str, params=()):
        cursor = self.dbid.cursor()
        cursor.execute(query_str, params)
        return cursor.fetchall()

    # The abstract methods from the Database class will be implemented here.
    # For brevity, I will start with a few key methods.

    def _do_get_branch_ids(self):
        rows = self.do_run_sql_query('SELECT DISTINCT branch_id FROM branches')
        return [row['branch_id'] for row in rows]

    def _do_add_branch(self, branch_id, parent_branch_id):
        import time
        cursor = self.dbid.cursor()

        # Handle empty string parent as NULL
        if parent_branch_id == '':
            parent_branch_id = None

        # Add the new branch
        cursor.execute('INSERT INTO branches (branch_id, parent_id, timestamp) VALUES (?, ?, ?)',
                       (branch_id, parent_branch_id, time.time()))

        # Copy docs from parent branch
        if parent_branch_id:
            cursor.execute('SELECT doc_idx FROM branch_docs WHERE branch_id = ?', (parent_branch_id,))
            doc_indices = [row['doc_idx'] for row in cursor.fetchall()]
            for doc_idx in doc_indices:
                cursor.execute('INSERT OR IGNORE INTO branch_docs (branch_id, doc_idx, timestamp) VALUES (?, ?, ?)',
                               (branch_id, doc_idx, time.time()))

        self.dbid.commit()

    def _do_get_doc_ids(self, branch_id=None):
        if branch_id:
            rows = self.do_run_sql_query('SELECT d.doc_id FROM docs d JOIN branch_docs bd ON d.doc_idx = bd.doc_idx WHERE bd.branch_id = ?', (branch_id,))
        else:
            rows = self.do_run_sql_query('SELECT doc_id FROM docs')
        return [row['doc_id'] for row in rows]

    def _do_add_doc(self, document_obj, branch_id, **kwargs):
        # This is a complex method that involves multiple steps:
        # 1. Check if the document already exists.
        # 2. If not, add it to the 'docs' table and get its 'doc_idx'.
        # 3. Add the document's fields to the 'doc_data' table.
        # 4. Add the document reference to the 'branch_docs' table.
        # This is a simplified placeholder.
        import json
        import time
        from ..document import Document

        doc_id = document_obj.id()
        cursor = self.dbid.cursor()

        cursor.execute('SELECT doc_idx FROM docs WHERE doc_id = ?', (doc_id,))
        row = cursor.fetchone()

        if row:
            doc_idx = row['doc_idx']
        else:
            json_code = json.dumps(document_obj.document_properties)
            cursor.execute('INSERT INTO docs (doc_id, json_code, timestamp) VALUES (?, ?, ?)',
                           (doc_id, json_code, time.time()))
            doc_idx = cursor.lastrowid
            # Simplified field insertion
            # A full implementation would parse the document and insert into doc_data

        try:
            cursor.execute('INSERT INTO branch_docs (branch_id, doc_idx, timestamp) VALUES (?, ?, ?)',
                           (branch_id, doc_idx, time.time()))
            self.dbid.commit()
        except sqlite3.IntegrityError as e:
            if "FOREIGN KEY" in str(e):
                raise ValueError(f"Branch '{branch_id}' does not exist.")
            # Ignore other integrity errors (duplicates)
            pass

    def _do_get_doc(self, document_id, OnMissing='error', **kwargs):
        from ..document import Document
        import json

        row = self.do_run_sql_query('SELECT json_code FROM docs WHERE doc_id = ?', (document_id,))

        if row:
            json_code = row[0]['json_code']
            doc_struct = json.loads(json_code)
            return Document(doc_struct)
        else:
            # Handle missing document
            if OnMissing == 'warn':
                print(f"Warning: Document id '{document_id}' not found.")
                return None
            elif OnMissing == 'ignore':
                return None
            else:
                raise ValueError(f"Document id '{document_id}' not found.")

    def open_doc(self, doc_id, filename):
        from ..file import ReadOnlyFileobj

        doc = self.get_docs(doc_id)
        if not doc:
            raise ValueError(f"Document {doc_id} not found.")

        is_in, info, _ = doc.is_in_file_list(filename)
        if is_in:
             location = info['locations']['location']

             # Rebase path if it's relative, assuming it's relative to the DB location
             if not os.path.isabs(location):
                 db_dir = os.path.dirname(os.path.abspath(self.connection))
                 location = os.path.join(db_dir, location)

             return ReadOnlyFileobj(location)

        raise FileNotFoundError(f"File {filename} not found in document {doc_id}.")

    def _do_remove_doc(self, document_id, branch_id, **kwargs):
        cursor = self.dbid.cursor()

        # Check if branch exists
        cursor.execute('SELECT 1 FROM branches WHERE branch_id = ?', (branch_id,))
        if not cursor.fetchone():
             raise ValueError(f"Branch '{branch_id}' does not exist.")

        # Get doc_idx from doc_id
        cursor.execute('SELECT doc_idx FROM docs WHERE doc_id = ?', (document_id,))
        row = cursor.fetchone()

        if row:
            doc_idx = row['doc_idx']
            # Remove from branch_docs
            cursor.execute('DELETE FROM branch_docs WHERE branch_id = ? AND doc_idx = ?', (branch_id, doc_idx))

            # Optional: remove from docs and doc_data if no other branches reference it
            cursor.execute('SELECT COUNT(*) FROM branch_docs WHERE doc_idx = ?', (doc_idx,))
            count = cursor.fetchone()[0]
            if count == 0:
                cursor.execute('DELETE FROM doc_data WHERE doc_idx = ?', (doc_idx,))
                cursor.execute('DELETE FROM docs WHERE doc_idx = ?', (doc_idx,))

            self.dbid.commit()
        else:
            # Handle missing document
            on_missing = kwargs.get('OnMissing', 'error').lower()
            if on_missing == 'warn':
                print(f"Warning: Document id '{document_id}' not found for removal.")
            elif on_missing != 'ignore':
                raise ValueError(f"Document id '{document_id}' not found for removal.")

    def _do_delete_branch(self, branch_id):
        cursor = self.dbid.cursor()
        cursor.execute('DELETE FROM branch_docs WHERE branch_id = ?', (branch_id,))
        cursor.execute('DELETE FROM branches WHERE branch_id = ?', (branch_id,))
        self.dbid.commit()

    def _do_get_sub_branches(self, branch_id):
        rows = self.do_run_sql_query('SELECT branch_id FROM branches WHERE parent_id = ?', (branch_id,))
        return [row['branch_id'] for row in rows]

    def _do_get_branch_parent(self, branch_id):
        row = self.do_run_sql_query('SELECT parent_id FROM branches WHERE branch_id = ?', (branch_id,))
        if row:
            return row[0]['parent_id']
        return None