"""A document removed from its last branch is removed completely.

Mirrors DID-matlab's tests/+did/+unittest/TestDocumentReclamation.m, and
covers DID-matlab issue #55 as ported here:

  1. the document's ingested files are deleted from disk
  2. the document's field (doc_data) records are deleted
  3. the document's id is retired and can never be added again

on both paths that can leave a document referenced by no branch: removing it
from its last branch, and deleting that branch. A document another branch
still holds must be left alone by all three, on both paths.
"""

import os
import shutil
import unittest

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB

SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "src", "did", "example_schema", "demo_schema1"
)


class TestDocumentReclamation(unittest.TestCase):
    DB_FILENAME = "test_document_reclamation.sqlite"

    def setUp(self):
        self.db_path = os.path.join(os.path.dirname(__file__), self.DB_FILENAME)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = SQLiteDB(self.db_path)
        self.db.add_branch("a")
        Document.set_schema_path(SCHEMA_PATH)

    def tearDown(self):
        self.db._close_db()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        shutil.rmtree(
            os.path.join(os.path.dirname(self.db_path), "files"), ignore_errors=True
        )

    # -- helpers ---------------------------------------------------------

    def query(self, sql, params=()):
        return self.db.do_run_sql_query(sql, params)

    def doc_idx(self, doc_id):
        rows = self.query("SELECT doc_idx FROM docs WHERE doc_id = ?", (doc_id,))
        return rows[0]["doc_idx"] if rows else None

    def cached_files_of(self, doc_idx):
        rows = self.query(
            "SELECT cached_location FROM files WHERE doc_idx = ?", (doc_idx,)
        )
        return [r["cached_location"] for r in rows if r["cached_location"]]

    def uncached_row_count_of(self, doc_idx):
        rows = self.query(
            "SELECT cached_location FROM files WHERE doc_idx = ?", (doc_idx,)
        )
        return sum(1 for r in rows if not r["cached_location"])

    def has_deleted_docs_table(self):
        return bool(
            self.query(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name = 'deleted_docs'"
            )
        )

    def make_file_document(self):
        """A demoFile document with an ingested file and an un-ingested one.

        add_file defaults a local path to ingest=1 and an http(s) one to
        ingest=0, so the document gets one files row with a cached copy and
        one without. That exercises both branches of the deletion loop: a row
        with a file to delete and a row with none.
        """
        doc = Document("demoFile")
        source = os.path.join(os.path.dirname(self.db_path), "reclaim_me.txt")
        with open(source, "w") as handle:
            handle.write("ingest me")
        doc.add_file("test_file.txt", source)
        doc.add_file("test_file.txt", "https://nosuchserver.com.notthere/test_file.txt")
        return doc

    # -- item 2: field data ----------------------------------------------

    def test_field_data_removed_with_last_branch_reference(self):
        doc = Document("demoA")
        self.db._do_add_doc(doc, "a")

        idx = self.doc_idx(doc.id())
        self.assertIsNotNone(idx, "setup: the docs row should exist")
        self.assertTrue(
            self.query("SELECT field_idx FROM doc_data WHERE doc_idx = ?", (idx,)),
            "setup: the document should have doc_data rows",
        )

        self.db.remove_docs(doc.id(), "a")

        self.assertIsNone(self.doc_idx(doc.id()))
        self.assertFalse(
            self.query("SELECT field_idx FROM doc_data WHERE doc_idx = ?", (idx,))
        )

    # -- item 1: ingested files ------------------------------------------

    def test_ingested_files_deleted_with_last_branch_reference(self):
        doc = self.make_file_document()
        self.db._do_add_doc(doc, "a")

        idx = self.doc_idx(doc.id())
        cached = self.cached_files_of(idx)
        self.assertTrue(cached, "setup: the document should have an ingested copy")
        for path in cached:
            self.assertTrue(os.path.isfile(path), f"setup: {path} should exist")
        self.assertGreater(
            self.uncached_row_count_of(idx),
            0,
            "setup: the URL location should give a files row with no cached copy",
        )

        self.db.remove_docs(doc.id(), "a")

        for path in cached:
            self.assertFalse(
                os.path.isfile(path), f"the ingested copy must be deleted: {path}"
            )
        self.assertFalse(self.query("SELECT uid FROM files WHERE doc_idx = ?", (idx,)))

    # -- item 3: retired ids ---------------------------------------------

    def test_removed_doc_id_cannot_be_added_again(self):
        doc = Document("demoA")
        self.db._do_add_doc(doc, "a")
        self.db.remove_docs(doc.id(), "a")

        with self.assertRaises(ValueError) as caught:
            self.db._do_add_doc(doc, "a")
        self.assertIn("previously removed", str(caught.exception))

    def test_retirement_survives_reopening_the_file(self):
        """The id is retired in the database, not just in memory."""
        doc = Document("demoA")
        self.db._do_add_doc(doc, "a")
        self.db.remove_docs(doc.id(), "a")

        self.db._close_db()
        self.db = SQLiteDB(self.db_path)

        with self.assertRaises(ValueError):
            self.db._do_add_doc(doc, "a")

    # -- the guard: another branch still holds it -------------------------

    def test_document_on_another_branch_is_untouched(self):
        doc = self.make_file_document()
        self.db._do_add_doc(doc, "a")

        idx = self.doc_idx(doc.id())
        cached = self.cached_files_of(idx)
        self.assertTrue(cached, "setup: the document should have an ingested copy")

        # 'a_a' inherits the document from 'a'
        self.db.add_branch("a_a", "a")
        self.db.remove_docs(doc.id(), "a_a")

        self.assertEqual(self.doc_idx(doc.id()), idx, "the docs row must survive")
        self.assertTrue(
            self.query("SELECT field_idx FROM doc_data WHERE doc_idx = ?", (idx,)),
            "the doc_data rows must survive",
        )
        for path in cached:
            self.assertTrue(
                os.path.isfile(path), f"the ingested copy must survive: {path}"
            )

    def test_id_still_on_another_branch_is_not_retired(self):
        doc = Document("demoA")
        self.db._do_add_doc(doc, "a")

        self.db.add_branch("a_a", "a")
        self.db.remove_docs(doc.id(), "a_a")

        # Not a deletion: branch 'a' still holds it, so the id stays usable
        self.db._do_add_doc(doc, "a_a")
        self.assertIsNotNone(self.db.get_docs(doc.id()))

    # -- the other orphaning path: deleting a branch ----------------------

    def test_deleting_last_branch_reclaims_its_documents(self):
        doc = self.make_file_document()
        self.db._do_add_doc(doc, "a")

        idx = self.doc_idx(doc.id())
        cached = self.cached_files_of(idx)
        self.assertTrue(cached, "setup: the document should have an ingested copy")

        # Deleting a branch that is NOT the last holder reclaims nothing
        self.db.add_branch("a_a", "a")
        self.db.delete_branch("a_a")

        self.assertEqual(
            self.doc_idx(doc.id()),
            idx,
            "deleting one branch must not reclaim a document another branch holds",
        )
        for path in cached:
            self.assertTrue(os.path.isfile(path), f"must survive: {path}")

        # Deleting the last branch that holds it reclaims it completely
        self.db.delete_branch("a")

        self.assertIsNone(self.doc_idx(doc.id()))
        self.assertFalse(
            self.query("SELECT field_idx FROM doc_data WHERE doc_idx = ?", (idx,))
        )
        self.assertFalse(self.query("SELECT uid FROM files WHERE doc_idx = ?", (idx,)))
        for path in cached:
            self.assertFalse(os.path.isfile(path), f"must be deleted: {path}")

        # Deleting the last branch left no current branch, so the new one is
        # a root. (Before delete_branch grew MATLAB's guards this had to pass
        # an explicit empty parent, because current_branch_id still named the
        # deleted branch and add_branch inherited it.)
        self.assertEqual(self.db.current_branch_id, "")

        # ...and its id is retired, as when remove_docs drops the last
        # reference
        self.db.add_branch("b")
        with self.assertRaises(ValueError):
            self.db._do_add_doc(doc, "b")

    # -- databases written before this existed ----------------------------

    def test_database_without_deleted_docs_table_still_works(self):
        """A database written before issue #55 must stay fully usable.

        Its absence can never be an error, and the table must appear only
        when a document is actually retired -- not merely because such a
        database was opened or read. MATLAB holds to the same contract, which
        is what lets the two languages share a database file.
        """
        cursor = self.db.dbid.cursor()
        cursor.execute("DROP TABLE deleted_docs")
        self.db.dbid.commit()
        self.assertFalse(self.has_deleted_docs_table(), "setup: table should be gone")

        # Adding must not care that the table is missing
        doc = Document("demoA")
        self.db._do_add_doc(doc, "a")

        # Reading must not create it either
        self.db.get_docs(doc.id())
        self.assertFalse(
            self.has_deleted_docs_table(),
            "reading a database must not add the deleted_docs table to it",
        )

        # Removing the last reference creates it on demand...
        self.db.remove_docs(doc.id(), "a")
        self.assertTrue(
            self.has_deleted_docs_table(),
            "retiring an id must create the deleted_docs table on demand",
        )

        # ...and the id is retired in it
        with self.assertRaises(ValueError):
            self.db._do_add_doc(doc, "a")


if __name__ == "__main__":
    unittest.main()
