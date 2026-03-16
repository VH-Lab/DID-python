import sqlite3
import os
import re as _re
from ..database import Database


def _sqlite_regexp(pattern, string):
    """SQLite regexp function implementation."""
    if string is None:
        return None
    try:
        return 1 if _re.search(pattern, str(string)) else None
    except _re.error:
        return None


def _sql_escape(value):
    """Escape single quotes for SQL string literals."""
    if value is None:
        return ""
    return str(value).replace("'", "''")


class SQLiteDB(Database):
    def __init__(self, filename):
        super().__init__(connection=filename)
        self.dbid = None
        self._fields_cache = {}  # (class, field_name) -> field_idx
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
        cursor.execute("""
            CREATE TABLE branches (
                branch_id TEXT NOT NULL UNIQUE,
                parent_id TEXT,
                timestamp REAL,
                FOREIGN KEY(parent_id) REFERENCES branches(branch_id),
                PRIMARY KEY(branch_id)
            )
        """)

        # Create docs table
        cursor.execute("""
            CREATE TABLE docs (
                doc_id TEXT NOT NULL UNIQUE,
                doc_idx INTEGER NOT NULL UNIQUE,
                json_code TEXT,
                timestamp REAL,
                PRIMARY KEY(doc_idx AUTOINCREMENT)
            )
        """)

        # Create branch_docs table
        cursor.execute("""
            CREATE TABLE branch_docs (
                branch_id TEXT NOT NULL,
                doc_idx INTEGER NOT NULL,
                timestamp REAL,
                FOREIGN KEY(branch_id) REFERENCES branches(branch_id),
                FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx),
                PRIMARY KEY(branch_id, doc_idx)
            )
        """)

        # Create fields table
        cursor.execute("""
            CREATE TABLE fields (
                class TEXT NOT NULL,
                field_name TEXT NOT NULL UNIQUE,
                json_name TEXT NOT NULL,
                field_idx INTEGER NOT NULL UNIQUE,
                PRIMARY KEY(field_idx AUTOINCREMENT)
            )
        """)

        # Create doc_data table
        cursor.execute("""
            CREATE TABLE doc_data (
                doc_idx INTEGER NOT NULL,
                field_idx INTEGER NOT NULL,
                value BLOB,
                FOREIGN KEY(doc_idx) REFERENCES docs(doc_idx),
                FOREIGN KEY(field_idx) REFERENCES fields(field_idx)
            )
        """)

        # Create files table
        cursor.execute("""
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
        """)

        self.dbid.commit()

    def do_run_sql_query(self, query_str, params=()):
        cursor = self.dbid.cursor()
        cursor.execute(query_str, params)
        return cursor.fetchall()

    # The abstract methods from the Database class will be implemented here.
    # For brevity, I will start with a few key methods.

    def _do_get_branch_ids(self):
        rows = self.do_run_sql_query("SELECT DISTINCT branch_id FROM branches")
        return [row["branch_id"] for row in rows]

    def _do_add_branch(self, branch_id, parent_branch_id):
        import time

        cursor = self.dbid.cursor()

        # Handle empty string parent as NULL
        if parent_branch_id == "":
            parent_branch_id = None

        # Add the new branch
        cursor.execute(
            "INSERT INTO branches (branch_id, parent_id, timestamp) VALUES (?, ?, ?)",
            (branch_id, parent_branch_id, time.time()),
        )

        # Copy docs from parent branch
        if parent_branch_id:
            cursor.execute(
                "SELECT doc_idx FROM branch_docs WHERE branch_id = ?",
                (parent_branch_id,),
            )
            doc_indices = [row["doc_idx"] for row in cursor.fetchall()]
            for doc_idx in doc_indices:
                cursor.execute(
                    "INSERT OR IGNORE INTO branch_docs (branch_id, doc_idx, timestamp) VALUES (?, ?, ?)",
                    (branch_id, doc_idx, time.time()),
                )

        self.dbid.commit()

    def _do_get_doc_ids(self, branch_id=None):
        if branch_id:
            rows = self.do_run_sql_query(
                "SELECT d.doc_id FROM docs d JOIN branch_docs bd ON d.doc_idx = bd.doc_idx WHERE bd.branch_id = ?",
                (branch_id,),
            )
        else:
            rows = self.do_run_sql_query("SELECT doc_id FROM docs")
        return [row["doc_id"] for row in rows]

    def _get_field_idx(self, cursor, group_name, field_name):
        """Look up or create a field_idx for the given group and field.

        The field_name in the fields table uses the format '{group}.{field}',
        matching MATLAB's convention. Triple-underscores in column names from
        doc2sql are converted to dots.
        """
        # Convert ___ back to . for the stored field_name
        full_field_name = f"{group_name}.{field_name}".replace("___", ".")
        json_name = full_field_name.replace(".", "___")

        cache_key = (group_name, full_field_name)
        if cache_key in self._fields_cache:
            return self._fields_cache[cache_key]

        cursor.execute(
            "SELECT field_idx FROM fields WHERE field_name = ?", (full_field_name,)
        )
        row = cursor.fetchone()
        if row:
            field_idx = row["field_idx"]
        else:
            cursor.execute(
                "INSERT INTO fields (class, field_name, json_name, field_idx) VALUES (?, ?, ?, NULL)",
                (group_name, full_field_name, json_name),
            )
            field_idx = cursor.lastrowid

        self._fields_cache[cache_key] = field_idx
        return field_idx

    def _populate_doc_data(self, cursor, doc_idx, document_obj):
        """Flatten document via doc2sql and insert into fields/doc_data tables."""
        from .doc2sql import doc_to_sql

        meta_tables = doc_to_sql(document_obj)
        rows = []

        for table in meta_tables:
            group_name = table["name"]
            for col in table["columns"]:
                col_name = col["name"]
                if col_name == "doc_id":
                    continue  # skip doc_id columns
                field_idx = self._get_field_idx(cursor, group_name, col_name)
                value = col["value"]
                if value is None:
                    value = ""
                rows.append((doc_idx, field_idx, str(value)))

        if rows:
            cursor.executemany(
                "INSERT INTO doc_data (doc_idx, field_idx, value) VALUES (?, ?, ?)",
                rows,
            )

    @staticmethod
    def _matlab_compatible_props(props):
        """Return a deep copy of props with single-element lists unwrapped to scalars.

        MATLAB's jsonencode converts single-element cell arrays to scalars.
        This replicates that behavior so DID-matlab can read Python-created databases.
        """
        import copy

        props = copy.deepcopy(props)

        # Unwrap document_class.superclasses
        dc = props.get("document_class", {})
        sc = dc.get("superclasses")
        if isinstance(sc, list) and len(sc) == 1:
            dc["superclasses"] = sc[0]

        # Unwrap depends_on
        dep = props.get("depends_on")
        if isinstance(dep, list) and len(dep) == 1:
            props["depends_on"] = dep[0]

        return props

    @staticmethod
    def _normalize_loaded_props(props):
        """Ensure superclasses and depends_on are always lists.

        Inverse of _matlab_compatible_props. Mutates and returns props.
        """
        dc = props.get("document_class", {})
        sc = dc.get("superclasses")
        if sc is not None and not isinstance(sc, list):
            dc["superclasses"] = [sc]

        dep = props.get("depends_on")
        if dep is not None and not isinstance(dep, list):
            props["depends_on"] = [dep]

        return props

    def _do_add_doc(self, document_obj, branch_id, **kwargs):
        import json
        import time

        doc_id = document_obj.id()
        cursor = self.dbid.cursor()

        cursor.execute("SELECT doc_idx FROM docs WHERE doc_id = ?", (doc_id,))
        row = cursor.fetchone()

        if row:
            doc_idx = row["doc_idx"]
        else:
            json_code = json.dumps(
                self._matlab_compatible_props(document_obj.document_properties)
            )
            cursor.execute(
                "INSERT INTO docs (doc_id, json_code, timestamp) VALUES (?, ?, ?)",
                (doc_id, json_code, time.time()),
            )
            doc_idx = cursor.lastrowid

            # Populate fields and doc_data tables (matching MATLAB's doc2sql behavior)
            self._populate_doc_data(cursor, doc_idx, document_obj)

        try:
            cursor.execute(
                "INSERT INTO branch_docs (branch_id, doc_idx, timestamp) VALUES (?, ?, ?)",
                (branch_id, doc_idx, time.time()),
            )
            self.dbid.commit()
        except sqlite3.IntegrityError as e:
            if "FOREIGN KEY" in str(e):
                raise ValueError(f"Branch '{branch_id}' does not exist.")
            # Ignore other integrity errors (duplicates)
            pass

    # --- SQL-based search (matching MATLAB's database.m) ---

    def search(self, query_obj, branch_id=None):
        """Search using SQL queries against doc_data, matching MATLAB's behavior."""
        if branch_id is None:
            branch_id = self.current_branch_id

        search_params = query_obj.to_search_structure()

        # Register regexp function for sqlite
        self.dbid.create_function("regexp", 2, _sqlite_regexp)

        doc_ids = self._search_doc_ids(search_params, branch_id)
        return doc_ids

    def _search_doc_ids(self, search_struct, branch_id):
        """Recursively search for doc_ids matching the search structure.

        Matches MATLAB's search_doc_ids: struct arrays are AND'd, 'or' operations
        are unioned, leaf queries go through SQL.
        """
        if isinstance(search_struct, list):
            if not search_struct:
                return []
            # AND: intersect results from all sub-queries
            result = None
            for item in search_struct:
                ids = self._search_doc_ids(item, branch_id)
                if result is None:
                    result = set(ids)
                else:
                    result &= set(ids)
            return list(result) if result else []

        if not isinstance(search_struct, dict):
            return []

        operation = search_struct.get("operation", "")
        negation = False
        op = operation
        if op.startswith("~"):
            negation = True
            op = op[1:]
        op_lower = op.lower()

        if op_lower == "or":
            # OR: union results from param1 and param2
            p1 = search_struct.get("param1")
            p2 = search_struct.get("param2")
            ids1 = self._search_doc_ids(p1, branch_id) if p1 else []
            ids2 = self._search_doc_ids(p2, branch_id) if p2 else []
            result = list(set(ids1) | set(ids2))
            if negation:
                all_ids = set(self._do_get_doc_ids(branch_id))
                result = list(all_ids - set(result))
            return result

        # Leaf query: build SQL and execute
        sql_clause = self._query_struct_to_sql_str(search_struct)
        if sql_clause is None:
            # Fallback to brute-force for unsupported operations
            return self._brute_force_search(search_struct, branch_id)

        query = (
            "SELECT DISTINCT docs.doc_id FROM docs, branch_docs, doc_data, fields "
            "WHERE docs.doc_idx = doc_data.doc_idx "
            "AND docs.doc_idx = branch_docs.doc_idx "
            "AND branch_docs.branch_id = ? "
            "AND fields.field_idx = doc_data.field_idx "
            f"AND {sql_clause}"
        )

        try:
            rows = self.do_run_sql_query(query, (branch_id,))
            matched = [row["doc_id"] for row in rows]
        except sqlite3.OperationalError:
            # Fallback on SQL error
            return self._brute_force_search(search_struct, branch_id)

        if negation:
            all_ids = set(self._do_get_doc_ids(branch_id))
            return list(all_ids - set(matched))

        return matched

    def _query_struct_to_sql_str(self, search_struct):
        """Convert a single query struct to a SQL WHERE clause fragment.

        Returns None if the operation is not supported in SQL.
        Matches MATLAB's query_struct_to_sql_str.
        """
        field = search_struct.get("field", "")
        operation = search_struct.get("operation", "")
        param1 = search_struct.get("param1")
        param2 = search_struct.get("param2")

        # Strip negation prefix (handled by caller)
        op = operation
        if op.startswith("~"):
            op = op[1:]
        op_lower = op.lower()

        if op_lower == "exact_string":
            return f"fields.field_name = '{field}' AND doc_data.value = '{_sql_escape(param1)}'"

        elif op_lower == "exact_string_anycase":
            return f"fields.field_name = '{field}' AND LOWER(doc_data.value) = LOWER('{_sql_escape(param1)}')"

        elif op_lower == "contains_string":
            return f"fields.field_name = '{field}' AND doc_data.value LIKE '%{_sql_escape(param1)}%'"

        elif op_lower == "regexp":
            return f"fields.field_name = '{field}' AND regexp('{_sql_escape(param1)}', doc_data.value) IS NOT NULL"

        elif op_lower == "exact_number":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) = {float(param1)}"

        elif op_lower == "lessthan":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) < {float(param1)}"

        elif op_lower == "lessthaneq":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) <= {float(param1)}"

        elif op_lower == "greaterthan":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) > {float(param1)}"

        elif op_lower == "greaterthaneq":
            return f"fields.field_name = '{field}' AND CAST(doc_data.value AS REAL) >= {float(param1)}"

        elif op_lower == "hasfield":
            return (
                f"(fields.field_name = '{field}' OR fields.field_name LIKE '{field}.%')"
            )

        elif op_lower == "isa":
            # isa: match on meta.class (exact) OR meta.superclass (contains)
            classname = _sql_escape(param1)
            return (
                f"((fields.field_name = 'meta.class' AND doc_data.value = '{classname}') "
                f"OR (fields.field_name = 'meta.superclass' AND "
                f"regexp('(^|, ){classname}(,|$)', doc_data.value) IS NOT NULL))"
            )

        elif op_lower == "depends_on":
            # depends_on: search meta.depends_on using LIKE '%name,value;%'
            name = _sql_escape(param1)
            value = _sql_escape(param2)
            if name == "*":
                return f"fields.field_name = 'meta.depends_on' AND doc_data.value LIKE '%,{value};%'"
            return f"fields.field_name = 'meta.depends_on' AND doc_data.value LIKE '%{name},{value};%'"

        elif op_lower == "hasanysubfield_exact_string":
            # Used by resolved depends_on - fall back to brute force
            return None

        elif op_lower == "hasanysubfield_contains_string":
            # Used by resolved isa - fall back to brute force
            return None

        elif op_lower == "hasmember":
            # hasmember on a stored value - fall back to brute force
            return None

        elif op_lower == "hassize":
            return None

        elif op_lower == "partial_struct":
            return None

        return None

    def _brute_force_search(self, search_struct, branch_id):
        """Fall back to brute-force field_search for unsupported SQL operations."""
        from ..datastructures import field_search

        doc_ids = self._do_get_doc_ids(branch_id)
        docs = self.get_docs(doc_ids, OnMissing="ignore")
        if docs is None:
            docs = []
        if not isinstance(docs, list):
            docs = [docs]

        matched = []
        for doc in docs:
            if doc and field_search(doc.document_properties, search_struct):
                matched.append(doc.id())
        return matched

    def _do_get_doc(self, document_id, OnMissing="error", **kwargs):
        from ..document import Document
        import json

        row = self.do_run_sql_query(
            "SELECT json_code FROM docs WHERE doc_id = ?", (document_id,)
        )

        if row:
            json_code = row[0]["json_code"]
            doc_struct = json.loads(json_code)
            doc_struct = self._normalize_loaded_props(doc_struct)
            return Document(doc_struct)
        else:
            # Handle missing document
            if OnMissing == "warn":
                print(f"Warning: Document id '{document_id}' not found.")
                return None
            elif OnMissing == "ignore":
                return None
            else:
                raise ValueError(f"Document id '{document_id}' not found.")

    def get_docs(self, document_ids, branch_id=None, OnMissing="error", **kwargs):
        """Bulk-fetch documents in a single SQL query.

        Overrides the base class one-at-a-time loop for efficiency.
        """
        from ..document import Document
        import json

        is_single = isinstance(document_ids, str)
        if is_single:
            document_ids = [document_ids]

        if not document_ids:
            return [] if not is_single else None

        # Filter by branch if requested
        if branch_id is not None:
            branch_doc_ids = set(self.get_doc_ids(branch_id))
            requested = []
            for doc_id in document_ids:
                if doc_id in branch_doc_ids:
                    requested.append(doc_id)
                elif OnMissing == "error":
                    raise ValueError(
                        f"Document {doc_id} not found in branch {branch_id}"
                    )
                elif OnMissing == "warn":
                    print(f"Warning: Document {doc_id} not found in branch {branch_id}")
            document_ids = requested

        if not document_ids:
            return [] if not is_single else None

        # Single SELECT ... WHERE doc_id IN (?, ?, ...)
        placeholders = ",".join("?" for _ in document_ids)
        rows = self.do_run_sql_query(
            f"SELECT doc_id, json_code FROM docs WHERE doc_id IN ({placeholders})",
            tuple(document_ids),
        )

        # Build lookup dict
        doc_map = {}
        for row in rows:
            doc_struct = json.loads(row["json_code"])
            doc_struct = self._normalize_loaded_props(doc_struct)
            doc_map[row["doc_id"]] = Document(doc_struct)

        # Preserve original order
        docs = []
        for doc_id in document_ids:
            if doc_id in doc_map:
                docs.append(doc_map[doc_id])
            elif OnMissing == "error":
                raise ValueError(f"Document id '{doc_id}' not found.")
            elif OnMissing == "warn":
                print(f"Warning: Document id '{doc_id}' not found.")

        if is_single:
            return docs[0] if docs else None
        return docs

    def get_docs_by_branch(self, branch_id=None):
        """Return all documents on a branch."""
        if branch_id is None:
            branch_id = self.current_branch_id
        doc_ids = self.get_doc_ids(branch_id)
        return self.get_docs(doc_ids, OnMissing="ignore")

    def open_doc(self, doc_id, filename):
        from ..file import ReadOnlyFileobj

        doc = self.get_docs(doc_id)
        if not doc:
            raise ValueError(f"Document {doc_id} not found.")

        is_in, info, _ = doc.is_in_file_list(filename)
        if is_in:
            location = info["locations"]["location"]

            # Rebase path if it's relative, assuming it's relative to the DB location
            if not os.path.isabs(location):
                db_dir = os.path.dirname(os.path.abspath(self.connection))
                location = os.path.join(db_dir, location)

            return ReadOnlyFileobj(location)

        raise FileNotFoundError(f"File {filename} not found in document {doc_id}.")

    def _do_remove_doc(self, document_id, branch_id, **kwargs):
        cursor = self.dbid.cursor()

        # Check if branch exists
        cursor.execute("SELECT 1 FROM branches WHERE branch_id = ?", (branch_id,))
        if not cursor.fetchone():
            raise ValueError(f"Branch '{branch_id}' does not exist.")

        # Get doc_idx from doc_id
        cursor.execute("SELECT doc_idx FROM docs WHERE doc_id = ?", (document_id,))
        row = cursor.fetchone()

        if row:
            doc_idx = row["doc_idx"]
            # Remove from branch_docs
            cursor.execute(
                "DELETE FROM branch_docs WHERE branch_id = ? AND doc_idx = ?",
                (branch_id, doc_idx),
            )

            # Optional: remove from docs and doc_data if no other branches reference it
            cursor.execute(
                "SELECT COUNT(*) FROM branch_docs WHERE doc_idx = ?", (doc_idx,)
            )
            count = cursor.fetchone()[0]
            if count == 0:
                cursor.execute("DELETE FROM doc_data WHERE doc_idx = ?", (doc_idx,))
                cursor.execute("DELETE FROM docs WHERE doc_idx = ?", (doc_idx,))

            self.dbid.commit()
        else:
            # Handle missing document
            on_missing = kwargs.get("OnMissing", "error").lower()
            if on_missing == "warn":
                print(f"Warning: Document id '{document_id}' not found for removal.")
            elif on_missing != "ignore":
                raise ValueError(f"Document id '{document_id}' not found for removal.")

    def _do_delete_branch(self, branch_id):
        cursor = self.dbid.cursor()
        cursor.execute("DELETE FROM branch_docs WHERE branch_id = ?", (branch_id,))
        cursor.execute("DELETE FROM branches WHERE branch_id = ?", (branch_id,))
        self.dbid.commit()

    def _do_get_sub_branches(self, branch_id):
        rows = self.do_run_sql_query(
            "SELECT branch_id FROM branches WHERE parent_id = ?", (branch_id,)
        )
        return [row["branch_id"] for row in rows]

    def _do_get_branch_parent(self, branch_id):
        row = self.do_run_sql_query(
            "SELECT parent_id FROM branches WHERE branch_id = ?", (branch_id,)
        )
        if row:
            return row[0]["parent_id"]
        return None
