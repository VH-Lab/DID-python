"""Removing a document that carries a file.

The ``files`` table was added on 2026-08-29 (``fix(sqlitedb): record file
locations so MATLAB can find them``) with ``FOREIGN KEY(doc_idx) REFERENCES
docs(doc_idx)`` and ``PRAGMA foreign_keys = ON``. ``_do_remove_doc`` was not
updated: it deletes the ``branch_docs``, ``doc_data`` and ``docs`` rows and
leaves the ``files`` rows pointing at a ``doc_idx`` that is about to go away,
so SQLite refuses the ``docs`` delete:

    sqlite3.IntegrityError: FOREIGN KEY constraint failed

Every document carrying a file was therefore un-removable. DID-matlab cannot
reach this: its ``do_remove_doc`` removes only the ``branch_docs`` row and
leaves ``docs``/``doc_data`` in place behind a ``% TODO``, so it never deletes
the row the foreign key points at. Python implements that TODO, which is why
only Python has to clean up after itself.

The failure also left the transaction dirty -- the ``branch_docs`` delete had
already run and nothing rolled it back, so the next successful commit anywhere
on the connection persisted a document unlinked from its branch while its
``docs`` row survived.
"""

import os
import tempfile
import unittest

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB


class TestRemoveDocWithFiles(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()
        self.db = SQLiteDB(os.path.join(self._dir, "t.sqlite"))
        self.db.add_branch("a")
        self.db.set_branch("a")

    def tearDown(self):
        self.db._close_db()

    def _file_doc(self, name="payload.bin", content=b"payload"):
        """A demoFile document carrying one real local file."""
        path = os.path.join(self._dir, name)
        with open(path, "wb") as handle:
            handle.write(content)
        doc = Document("demoFile")
        doc.add_file(name, path)
        return doc

    def _rows(self, table, doc_idx=None):
        if doc_idx is None:
            return self.db.do_run_sql_query(f"SELECT * FROM {table}")
        return self.db.do_run_sql_query(
            f"SELECT * FROM {table} WHERE doc_idx = ?", (doc_idx,)
        )

    def _doc_idx(self, doc_id):
        rows = self.db.do_run_sql_query(
            "SELECT doc_idx FROM docs WHERE doc_id = ?", (doc_id,)
        )
        return rows[0]["doc_idx"] if rows else None

    def test_remove_doc_with_a_file(self):
        doc = self._file_doc()
        self.db.add_docs([doc], "a", validate=False)

        doc_idx = self._doc_idx(doc.id())
        self.assertIsNotNone(doc_idx)
        self.assertTrue(self._rows("files", doc_idx), "no files row was written")

        self.db.remove_docs([doc.id()], "a")

        self.assertEqual(self.db.get_doc_ids("a"), [])
        self.assertIsNone(self._doc_idx(doc.id()))

    def test_files_rows_do_not_outlive_their_document(self):
        """An orphan files row would collide: uid is UNIQUE across the table."""
        doc = self._file_doc()
        self.db.add_docs([doc], "a", validate=False)
        self.db.remove_docs([doc.id()], "a")

        self.assertEqual(self._rows("files"), [], "files rows outlived the document")
        self.assertEqual(self._rows("doc_data"), [])

    def test_a_document_on_two_branches_keeps_its_files(self):
        """Only the last branch reference takes the rows down, as for doc_data."""
        doc = self._file_doc()
        self.db.add_docs([doc], "a", validate=False)
        self.db.add_branch("b", "a")
        self.db.add_docs([doc], "b", validate=False, OnDuplicate="ignore")

        self.db.remove_docs([doc.id()], "a")

        doc_idx = self._doc_idx(doc.id())
        self.assertIsNotNone(doc_idx, "the document was removed from branch b too")
        self.assertTrue(
            self._rows("files", doc_idx), "files rows went with the first branch"
        )

        self.db.remove_docs([doc.id()], "b")
        self.assertIsNone(self._doc_idx(doc.id()))
        self.assertEqual(self._rows("files"), [])

    def test_a_failed_remove_leaves_nothing_half_done(self):
        """A raising remove must not leave the branch link deleted."""
        doc = self._file_doc()
        self.db.add_docs([doc], "a", validate=False)
        doc_idx = self._doc_idx(doc.id())

        real_connection = self.db.dbid

        class _FailingCursor:
            """Passes every statement through except the docs delete."""

            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def execute(self, sql, *args):
                if sql.startswith("DELETE FROM docs"):
                    raise RuntimeError("boom")
                return self._inner.execute(sql, *args)

        class _FailingConnection:
            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def cursor(self):
                return _FailingCursor(self._inner.cursor())

        self.db.dbid = _FailingConnection(real_connection)
        try:
            with self.assertRaises(RuntimeError):
                self.db.remove_docs([doc.id()], "a")
        finally:
            self.db.dbid = real_connection

        self.assertEqual(
            self.db.get_doc_ids("a"),
            [doc.id()],
            "the branch link was deleted even though the remove failed",
        )
        self.assertTrue(self._rows("files", doc_idx))


if __name__ == "__main__":
    unittest.main()
