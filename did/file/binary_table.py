import sqlite3
import os

class BinaryTable:
    def __init__(self, f, record_type, record_size, elements_per_column, header_size):
        self.db_filename = f.fullpathfilename
        self.record_type = record_type
        self.header_size = header_size
        self._conn = None

        self._create_tables()

    def _get_conn(self):
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_filename)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _create_tables(self):
        conn = self._get_conn()
        cursor = conn.cursor()

        # Header table
        cursor.execute("CREATE TABLE IF NOT EXISTS header (key TEXT PRIMARY KEY, value BLOB)")

        # Data table
        columns = ', '.join([f'col{i} {self._py_type(self.record_type[i])}' for i in range(len(self.record_type))])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY AUTOINCREMENT, {columns})")

        conn.commit()

    def _py_type(self, matlab_type):
        if matlab_type == 'char':
            return 'TEXT'
        elif matlab_type == 'double':
            return 'REAL'
        elif matlab_type == 'uint64':
            return 'INTEGER'
        else:
            return 'BLOB'

    def get_size(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM data")
        r = cursor.fetchone()[0]
        c = len(self.record_type)
        return r, c, self.header_size + r * sum(self.record_size) if hasattr(self, 'record_size') else 0

    def read_header(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM header WHERE key = 'header_data'")
        row = cursor.fetchone()
        return row['value'] if row else None

    def write_header(self, header_data):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO header (key, value) VALUES (?, ?)", ('header_data', header_data))
        conn.commit()

    def row_size(self):
        return sum(self.record_size) if hasattr(self, 'record_size') else 0

    def read_row(self, row, col):
        conn = self._get_conn()
        cursor = conn.cursor()

        if isinstance(row, float) and row == float('inf'):
            cursor.execute(f"SELECT col{col-1} FROM data")
            return [r[f'col{col-1}'] for r in cursor.fetchall()]

        if not isinstance(row, list):
            row = [row]

        placeholders = ','.join('?' for _ in row)
        cursor.execute(f"SELECT col{col-1} FROM data WHERE id IN ({placeholders})", row)
        return [r[f'col{col-1}'] for r in cursor.fetchall()]

    def insert_row(self, insert_after, data_cell):
        conn = self._get_conn()
        cursor = conn.cursor()
        columns = ', '.join([f'col{i}' for i in range(len(data_cell))])
        placeholders = ', '.join('?' for _ in data_cell)
        cursor.execute(f"INSERT INTO data ({columns}) VALUES ({placeholders})", data_cell)
        conn.commit()

    def delete_row(self, row):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM data WHERE id = ?", (row,))
        conn.commit()

    def write_table(self, data):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM data")

        if not data:
            conn.commit()
            return

        columns = ', '.join([f'col{i}' for i in range(len(data[0]))])
        placeholders = ', '.join('?' for _ in data[0])
        cursor.executemany(f"INSERT INTO data ({columns}) VALUES ({placeholders})", data)
        conn.commit()

    def find_row(self, col, value, sorted=False):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT id FROM data WHERE col{col-1} = ?", (value,))
        row = cursor.fetchone()
        if row:
            return row['id'], row['id'] -1 if row['id'] > 1 else 0

        r, _, _ = self.get_size()
        return 0, r

    def write_entry(self, row, col, value):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(f"UPDATE data SET col{col-1} = ? WHERE id = ?", (value, row))
        conn.commit()
