import contextlib
import os
import re as _re
import sqlite3
import struct

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


# Escape character used in LIKE patterns (see _sql_like_escape / ESCAPE clauses).
_LIKE_ESCAPE_CHAR = "\\"


def _sql_like_escape(value):
    """Escape LIKE wildcards in a literal operand of a LIKE pattern.

    '%' and '_' are LIKE wildcards; without escaping, a field name containing
    '_' would match any single character (e.g. 'a_b' would also match 'axb'),
    producing false-positive matches. Callers that embed the result inside a
    LIKE pattern must also append "ESCAPE '\\'" so the backslash is treated as
    the escape character. The single-quote escaping for the surrounding SQL
    string literal is applied on top of this by _sql_escape.
    """
    if value is None:
        return ""
    text = str(value)
    text = text.replace(_LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
    text = text.replace("%", _LIKE_ESCAPE_CHAR + "%")
    text = text.replace("_", _LIKE_ESCAPE_CHAR + "_")
    return text


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
        # Always ensure the search-critical indexes exist — including on
        # databases created before this fix (idempotent).
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create the indexes the document search depends on, if missing.

        The search is a 4-table join over docs/branch_docs/doc_data/fields
        that filters on ``doc_data.field_idx`` + ``doc_data.value`` and joins
        on ``doc_data.doc_idx``. ``doc_data`` (one row per field per doc — by
        far the largest table) had NO index, so every search did a full-scan
        nested-loop join — pathologically slow on large datasets (a single
        ``getprobes`` took ~70 s on a 115 MB cloud dataset).

        This restores the indexing the DID-MATLAB reference already has
        (sqlitedb.m creates ``doc_data(value)``) — the Python port dropped
        it — and adds ``doc_data(field_idx, value)`` (more targeted for the
        actual field+value search) and ``doc_data(doc_idx)`` (the doc join).
        Run on every open (``IF NOT EXISTS``) so databases downloaded before
        this fix benefit on next use. (docs.doc_id and fields.field_name are
        already covered by UNIQUE constraints.)
        """
        cursor = self.dbid.cursor()
        cursor.execute('CREATE INDEX IF NOT EXISTS "doc_data_value" ON doc_data(value)')
        cursor.execute(
            'CREATE INDEX IF NOT EXISTS "doc_data_field_value" '
            "ON doc_data(field_idx, value)"
        )
        cursor.execute(
            'CREATE INDEX IF NOT EXISTS "doc_data_doc_idx" ON doc_data(doc_idx)'
        )
        self.dbid.commit()

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

        # Unwrap files.file_info
        files = props.get("files")
        if isinstance(files, dict):
            fi = files.get("file_info")
            if isinstance(fi, list) and len(fi) == 1:
                files["file_info"] = fi[0]

        return props

    @staticmethod
    def _normalize_loaded_props(props):
        """Ensure superclasses, depends_on, file_info, and locations are always lists.

        Inverse of _matlab_compatible_props. Mutates and returns props.
        """
        dc = props.get("document_class", {})
        sc = dc.get("superclasses")
        if sc is not None and not isinstance(sc, list):
            dc["superclasses"] = [sc]

        dep = props.get("depends_on")
        if dep is not None and not isinstance(dep, list):
            props["depends_on"] = [dep]

        # Re-wrap files.file_info (but not locations, which may be a bare dict
        # from add_file in the Python API)
        files = props.get("files")
        if isinstance(files, dict):
            fi = files.get("file_info")
            if isinstance(fi, dict):
                files["file_info"] = [fi]

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

        # Record the document's file locations. MATLAB's do_open_doc resolves a
        # file by querying `docs, files` -- it never reads the locations back
        # out of the document JSON -- so without these rows every file in a
        # Python-written document is unreachable from MATLAB, including a plain
        # local file that exists on disk.
        self._populate_files(cursor, doc_idx, document_obj)

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

    @staticmethod
    def _file_entries(document_obj):
        """Yield (filename, location_entry) for every location of every file."""
        files = document_obj.document_properties.get("files")
        if not isinstance(files, dict):
            return
        file_info = files.get("file_info")
        if isinstance(file_info, dict):
            file_info = [file_info]
        if not isinstance(file_info, list):
            return

        for info in file_info:
            if not isinstance(info, dict):
                continue
            name = info.get("name")
            if not isinstance(name, str) or not name:
                continue
            locations = info.get("locations")
            if isinstance(locations, dict):
                locations = [locations]
            if not isinstance(locations, list):
                continue
            for entry in locations:
                if isinstance(entry, dict) and entry.get("location"):
                    yield name, entry

    def _populate_files(self, cursor, doc_idx, document_obj):
        """Insert one `files` row per file location, mirroring MATLAB.

        Columns match MATLAB's insert exactly: doc_idx, filename, uid,
        orig_location, cached_location, type, parameters.

        cached_location is always empty here. MATLAB fills it when it ingests a
        location into its FileDir; DID-python does no ingestion, so there is
        never a cached copy to point at. That is a real difference in what the
        two write, not a gap in this row -- see PORTING_INSTRUCTIONS.md.

        INSERT OR IGNORE because a document added to a second branch reaches
        this path again with the same (doc_idx, filename, uid) primary key.
        """
        for name, entry in self._file_entries(document_obj):
            cursor.execute(
                "INSERT OR IGNORE INTO files "
                "(doc_idx, filename, uid, orig_location, cached_location, "
                "type, parameters) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    doc_idx,
                    name,
                    str(entry.get("uid", "")),
                    str(entry.get("location", "")),
                    "",
                    str(entry.get("location_type", "file")),
                    str(entry.get("parameters", "")),
                ),
            )

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
        try:
            sql_clause = self._query_struct_to_sql_str(search_struct)
        except (ValueError, TypeError):
            # A numeric operation (exact_number/lessthan/greaterthan/...) was
            # given a non-numeric param1, so the float() conversion failed.
            # Fall back to brute force rather than aborting the whole search.
            return self._brute_force_search(search_struct, branch_id)
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
        op = op.removeprefix("~")
        op_lower = op.lower()

        # The query field name is interpolated into the SQL text below (e.g.
        # fields.field_name = '<field>'); it cannot be a bound '?' parameter
        # because it also appears in LIKE patterns. Restrict it to the
        # dotted-identifier charset so a crafted field name cannot break out of
        # the quoting. An out-of-charset field returns None here and the caller
        # falls back to the (injection-free) brute-force search.
        if field and not _re.fullmatch(r"[A-Za-z0-9_.]+", field):
            return None

        if op_lower == "exact_string":
            return f"fields.field_name = '{field}' AND doc_data.value = '{_sql_escape(param1)}'"

        elif op_lower == "exact_string_anycase":
            return f"fields.field_name = '{field}' AND LOWER(doc_data.value) = LOWER('{_sql_escape(param1)}')"

        elif op_lower == "contains_string":
            # param1 is arbitrary user text: '%' and '_' in it must NOT act as
            # LIKE wildcards (otherwise 'spike_sort' would also match
            # 'spikeXsort'), which would diverge from the brute-force
            # ``param1 in value`` path. Escape the LIKE wildcards, add an ESCAPE
            # clause, and SQL-escape the surrounding literal. The bracketing
            # '%' are the real "contains" wildcards and stay unescaped.
            param_like = _sql_escape(_sql_like_escape(param1))
            return (
                f"fields.field_name = '{field}' AND doc_data.value "
                f"LIKE '%{param_like}%' ESCAPE '{_LIKE_ESCAPE_CHAR}'"
            )

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
            # 'field' is charset-restricted above, but it may legitimately
            # contain '_', which is a LIKE wildcard. Escape LIKE wildcards in
            # the literal prefix and add an ESCAPE clause so a field name like
            # 'a_b' matches 'a_b[.subfield]' exactly, not 'axb'. The trailing
            # '.%' is a real wildcard and is left unescaped.
            field_like = _sql_like_escape(field)
            return (
                f"(fields.field_name = '{field}' "
                f"OR fields.field_name LIKE '{field_like}.%' ESCAPE '{_LIKE_ESCAPE_CHAR}')"
            )

        elif op_lower == "isa":
            # isa: match on meta.class (exact) OR meta.superclass (contains).
            # The meta.class branch is an exact string compare, so it only
            # needs SQL-literal escaping. The meta.superclass branch embeds the
            # class name inside a regexp() pattern; regex metacharacters in the
            # class name (e.g. '.') must be regex-escaped first, otherwise a
            # name like 'foo.bar' would also match 'fooxbar'. Anchor it as an
            # exact list-element match between the '(^|, )' / '(,|$)' delimiters.
            classname = _sql_escape(param1)
            classname_re = _sql_escape(
                _re.escape("" if param1 is None else str(param1))
            )
            return (
                f"((fields.field_name = 'meta.class' AND doc_data.value = '{classname}') "
                f"OR (fields.field_name = 'meta.superclass' AND "
                f"regexp('(^|, ){classname_re}(,|$)', doc_data.value) IS NOT NULL))"
            )

        elif op_lower == "depends_on":
            # depends_on: search meta.depends_on using LIKE '%name,value;%'.
            # NOTE: this branch is currently unreachable — Query._resolve_single
            # rewrites every 'depends_on' into 'hasanysubfield_exact_string'
            # before it reaches here, so depends_on always brute-forces. The
            # LIKE-wildcard escaping below is applied defensively so that if the
            # resolution is ever changed to let this branch run, '_'/'%' in the
            # dependency name/value can't silently act as wildcards. The ','/';'
            # delimiters and the bracketing '%' are the real pattern structure
            # and stay unescaped.
            name = _sql_escape(_sql_like_escape(param1))
            value = _sql_escape(_sql_like_escape(param2))
            if param1 == "*":
                return (
                    "fields.field_name = 'meta.depends_on' AND doc_data.value "
                    f"LIKE '%,{value};%' ESCAPE '{_LIKE_ESCAPE_CHAR}'"
                )
            return (
                "fields.field_name = 'meta.depends_on' AND doc_data.value "
                f"LIKE '%{name},{value};%' ESCAPE '{_LIKE_ESCAPE_CHAR}'"
            )

        elif op_lower == "hasanysubfield_exact_string":
            # unreachable — see Query._resolve_single (resolved depends_on);
            # falls back to brute force
            return None

        elif op_lower == "hasanysubfield_contains_string":
            # Used by resolved isa - fall back to brute force
            return None

        elif op_lower == "hasmember":
            # hasmember on a stored value - fall back to brute force
            return None

        elif op_lower == "hassize" or op_lower == "partial_struct":
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
        import json

        from ..document import Document

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
        import json

        from ..document import Document

        is_single = isinstance(document_ids, str)
        if is_single:
            document_ids = [document_ids]

        if not document_ids:
            return [] if not is_single else None

        # Fetch the requested docs in ONE indexed query, restricting to the
        # branch via a JOIN.
        #
        # PERF: the previous code fetched ALL of the branch's doc_ids
        # (``set(self.get_doc_ids(branch_id))``, O(total_docs)) and filtered
        # the small ``document_ids`` list in Python. NDI's ``epochtable``
        # calls ``get_docs`` once per epoch, so that was O(epochs x
        # total_docs) — a single ``getprobes`` took minutes on a large cloud
        # dataset (the live NDI spike-sort hung here). The branch JOIN +
        # ``doc_id IN (...)`` is O(len(document_ids)) using the branch_docs
        # and docs indexes. Branch membership is enforced by the JOIN; docs
        # not in the branch simply aren't returned and are handled by the
        # OnMissing pass below (same behaviour as before).
        # Chunk the IN-list: SQLite caps host parameters per statement
        # (SQLITE_MAX_VARIABLE_NUMBER — 999 on older builds), so a get_docs
        # over thousands of ids (e.g. a cross-document query on a large cloud
        # dataset) would raise "too many SQL variables". Batch under the limit
        # and accumulate; order is restored from doc_map below.
        _CHUNK = 900
        rows = []
        for _i in range(0, len(document_ids), _CHUNK):
            chunk = document_ids[_i : _i + _CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            if branch_id is not None:
                rows.extend(
                    self.do_run_sql_query(
                        f"SELECT d.doc_id, d.json_code FROM docs d "
                        f"JOIN branch_docs bd ON d.doc_idx = bd.doc_idx "
                        f"WHERE bd.branch_id = ? AND d.doc_id IN ({placeholders})",
                        (branch_id, *chunk),
                    )
                )
            else:
                rows.extend(
                    self.do_run_sql_query(
                        f"SELECT doc_id, json_code FROM docs WHERE doc_id IN ({placeholders})",
                        tuple(chunk),
                    )
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

    @staticmethod
    def _is_remote_location(location, location_type=None):
        """Is this location one that has to be retrieved rather than opened?

        Anything carrying a URI scheme -- ndic://, https://, s3:// -- is
        remote. DID retrieves none of them itself; see open_doc.
        """
        if str(location_type or "").lower() in ("url", "ndicloud"):
            return True
        return "://" in location

    def _resolve_local(self, location):
        """Absolute path for a local location, rebased against the db dir."""
        if os.path.isabs(location):
            return location
        db_dir = os.path.dirname(os.path.abspath(self.connection))
        return os.path.join(db_dir, location)

    def _locations_from_files_table(self, doc_id, filename):
        """Locations for one file, read from the `files` table.

        This is where MATLAB keeps them, and it is the only place it looks:
        do_open_doc selects cached_location, orig_location, uid and type from
        `docs, files`. A MATLAB-written document that was ingested has had its
        original deleted, so the document JSON's location no longer exists and
        only these rows can find the file.

        Returns a list shaped like the document's own locations, so both
        sources feed the same resolution path.
        """
        try:
            cursor = self.dbid.cursor()
            rows = cursor.execute(
                "SELECT f.uid, f.orig_location, f.cached_location, f.type "
                "FROM docs d, files f "
                "WHERE d.doc_id = ? AND f.doc_idx = d.doc_idx AND f.filename = ?",
                (doc_id, filename),
            ).fetchall()
        except sqlite3.Error:
            return []

        cache = self._file_cache()

        locations = []
        for row in rows:
            uid = row["uid"]
            cached = row["cached_location"]
            # The global file cache comes first, as it does in MATLAB's
            # do_open_doc: a file retrieved from a remote location once is
            # kept there under its uid, so a second open of the same document
            # does not fetch it again.
            if uid and cache is not None:
                locations.append(
                    {
                        "location": cache.full_path(str(uid)),
                        "location_type": "file",
                        "uid": uid,
                        "in_file_cache": True,
                    }
                )
            # MATLAB stores an ingested copy at <FileDir>/<uid>, where FileDir
            # is <directory holding the .sqlite file>/files. It looks the copy
            # up by uid rather than by cached_location, so mirror that next.
            if uid:
                locations.append(
                    {
                        "location": os.path.join(self._file_dir(), str(uid)),
                        "location_type": "file",
                        "uid": uid,
                    }
                )
            if cached:
                locations.append({"location": cached, "location_type": "file"})
            if row["orig_location"]:
                locations.append(
                    {
                        "location": row["orig_location"],
                        "location_type": row["type"] or "file",
                        "uid": uid,
                    }
                )
        return locations

    def _file_dir(self):
        """MATLAB's FileDir: `files/` beside the database file."""
        return os.path.join(os.path.dirname(os.path.abspath(self.connection)), "files")

    @staticmethod
    def _file_cache():
        """The process-wide file cache, or None if it cannot be opened.

        MATLAB's do_open_doc calls did.common.getCache() unconditionally.
        Here a cache that cannot be created -- an unwritable home directory,
        a corrupt .fileCacheInfo -- must not make documents unopenable, since
        the cache is an optimization: without it every remote file is simply
        fetched again. So this degrades to None and open_doc carries on.
        """
        from ..common import get_cache

        try:
            return get_cache()
        except (OSError, ValueError, struct.error):
            return None

    @staticmethod
    def _valid_locations(info):
        """The document's locations for one file, always as a list of dicts.

        A document may list several locations for one file -- the shipped
        demoFile.json template carries a local path and a remote one for each
        -- and a single-location document stores a bare dict rather than a
        one-element list.
        """
        locations = info.get("locations")
        if isinstance(locations, dict):
            locations = [locations]
        elif not isinstance(locations, list):
            return []
        return [
            entry
            for entry in locations
            if isinstance(entry, dict)
            and isinstance(entry.get("location", ""), str)
            and entry.get("location")
        ]

    def open_doc(self, doc_id, filename, custom_file_handler=None):
        """Open a file belonging to a document.

        Local locations are opened directly. A remote location is handed to
        ``custom_file_handler(dest_path, source_path)``, which must produce a
        local file at ``dest_path``; this mirrors MATLAB's ``customFileHandler``
        name-value argument to ``do_open_doc``, and is how a downstream package
        supplies retrieval without DID depending on it. DID itself downloads
        nothing, in either language.

        Raises FileAccessError -- which carries MATLAB's error identifier and
        subclasses FileNotFoundError -- when no location can be reached.
        """
        from ..common import PathConstants
        from ..database import FileAccessError
        from ..file import ReadOnlyFileobj

        doc = self.get_docs(doc_id)
        if not doc:
            raise ValueError(f"Document {doc_id} not found.")

        is_in, info, _ = doc.is_in_file_list(filename)
        if not is_in:
            raise FileNotFoundError(f"File {filename} not found in document {doc_id}.")

        # The files table first, since that is the only place MATLAB records an
        # ingested copy; then the document's own locations, which is all a
        # database written before this table was populated will have.
        locations = self._locations_from_files_table(doc_id, filename)
        locations += self._valid_locations(info)

        # First pass: anything already on disk, as MATLAB does before it tries
        # to retrieve anything.
        remote = []
        for entry in locations:
            location = entry["location"]
            if self._is_remote_location(location, entry.get("location_type")):
                remote.append(entry)
                continue
            resolved = self._resolve_local(location)
            if os.path.isfile(resolved):
                if entry.get("in_file_cache"):
                    # Record the use, or the cache would evict what is read
                    # most as though it had never been read at all.
                    cache = self._file_cache()
                    if cache is not None:
                        with contextlib.suppress(OSError, ValueError):
                            cache.touch(str(entry["uid"]))
                return ReadOnlyFileobj(resolved)

        # Second pass: hand each remote location to the caller's retriever.
        if remote and custom_file_handler is not None:
            temp_dir = PathConstants().temppath
            failures = []
            for entry in remote:
                location = entry["location"]
                uid = entry.get("uid") or os.path.basename(location) or "did_download"
                dest_path = os.path.join(temp_dir, str(uid))

                # Clear any earlier download at this path first. temppath
                # persists between calls and uids are only unique per
                # document, so without this a stale file would be served as
                # though freshly retrieved -- and a handler that produced
                # nothing would look like it had succeeded.
                if os.path.exists(dest_path):
                    try:
                        os.remove(dest_path)
                    except OSError as error:
                        failures.append(
                            f"{location}: cannot clear {dest_path}: {error}"
                        )
                        continue

                try:
                    custom_file_handler(dest_path, location)
                except Exception as error:  # noqa: BLE001 - reported below
                    failures.append(f"{location}: {error}")
                    continue
                if os.path.isfile(dest_path):
                    cached_path = self._add_to_file_cache(dest_path, uid)
                    return ReadOnlyFileobj(cached_path or dest_path)
                failures.append(
                    f"{location}: custom_file_handler did not produce a file "
                    f'at "{dest_path}"'
                )
            raise FileAccessError(
                "DID:SQLITEDB:FileRetrieval:CustomHandlerFailed",
                f'The file "{filename}" in document "{doc_id}" could not be '
                f"retrieved: " + "; ".join(failures),
            )

        # Nothing was reachable. Raise rather than hand back a Fileobj over a
        # path that does not exist: Fileobj.fopen() swallows the OSError and
        # leaves fid None, so a caller that does not check it reads b"" and
        # sees no error at all.
        if remote and len(remote) == len(locations):
            raise FileAccessError(
                "DID:SQLITEDB:FileRetrieval:UnsupportedType",
                f'The file "{filename}" in document "{doc_id}" is only '
                f'available remotely ({remote[0]["location"]}) and no '
                f"custom_file_handler was supplied. DID does not download "
                f"files itself; a downstream package supplies retrieval "
                f"through custom_file_handler. See PORTING_INSTRUCTIONS.md.",
            )
        raise FileAccessError(
            "DID:SQLITEDB:open",
            f'The file "{filename}" in document "{doc_id}" cannot be accessed.',
        )

    def _add_to_file_cache(self, source_path, uid):
        """Move a freshly retrieved file into the file cache.

        Returns its path inside the cache, or None if it could not be cached
        -- in which case the caller keeps using the file where it landed.
        Failing to cache is never a reason to fail the open: the file was
        retrieved, and the only cost is retrieving it again next time.
        """
        cache = self._file_cache()
        if cache is None or not uid:
            return None
        name = str(uid)
        if len(name) != cache.file_name_characters:
            # The cache's rows are fixed-width, so a uid of another length
            # cannot be indexed. Nothing is lost but the caching.
            return None
        try:
            if cache.is_file(name):
                cache.remove_file(name)
            cache.add_file(source_path, name)
        except (OSError, ValueError, struct.error):
            return None
        return cache.full_path(name)

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
