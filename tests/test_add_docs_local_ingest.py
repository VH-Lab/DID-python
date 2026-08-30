"""Local ingestion at add time: copy into FileDir, honor delete_original.

Ports the `'file'` branch of MATLAB's `do_add_doc` ingestion loop
(`+did/+implementations/sqlitedb.m`). Every location whose `ingest` flag is
set gets a copy at ``<FileDir>/<uid>`` recorded in `files.cached_location`;
a local one is copied, and on success `delete_original` deletes the source.

`Document.add_file` defaults both flags on for a local path, so this is the
ordinary path for a locally added file, not an edge case.

The remote half of the same loop is in test_add_docs_file_handler.py.
"""

import os
import tempfile
import unittest
import warnings

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB


class TestLocalIngest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()
        self.db = SQLiteDB(os.path.join(self._dir, "t.sqlite"))
        self.db.add_branch("a")
        self.db.set_branch("a")

    def tearDown(self):
        self.db._close_db()

    def _source(self, name="source.bin", content=b"payload"):
        path = os.path.join(self._dir, name)
        with open(path, "wb") as handle:
            handle.write(content)
        return path

    def _doc(self, location, ingest=1, delete_original=1, uid="u-local"):
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", "placeholder", ingest=0, delete_original=0)
        doc.add_file("filename2.ext", "placeholder", ingest=0, delete_original=0)
        is_in, info, _ = doc.is_in_file_list("filename1.ext")
        self.assertTrue(is_in)
        info["locations"] = [
            {
                "location": location,
                "location_type": "file",
                "uid": uid,
                "ingest": ingest,
                "delete_original": delete_original,
                "parameters": "",
            }
        ]
        return doc

    def _row(self, doc_id, filename="filename1.ext"):
        return (
            self.db.dbid.cursor()
            .execute(
                "SELECT f.uid, f.orig_location, f.cached_location FROM docs d, files f "
                "WHERE d.doc_id = ? AND f.doc_idx = d.doc_idx AND f.filename = ?",
                (doc_id, filename),
            )
            .fetchone()
        )

    # -- the copy -----------------------------------------------------------

    def test_a_local_file_is_copied_into_file_dir(self):
        source = self._source()
        doc = self._doc(source, delete_original=0)

        self.db.add_docs([doc], validate=False)

        dest = os.path.join(self.db._file_dir(), "u-local")
        self.assertTrue(os.path.isfile(dest), "the copy should be at <FileDir>/<uid>")
        with open(dest, "rb") as handle:
            self.assertEqual(handle.read(), b"payload")
        self.assertEqual(self._row(doc.id())["cached_location"], dest)

    def test_the_original_location_is_still_recorded(self):
        """orig_location keeps pointing at where the file came from."""
        source = self._source()
        doc = self._doc(source, delete_original=0)

        self.db.add_docs([doc], validate=False)

        self.assertEqual(self._row(doc.id())["orig_location"], source)

    def test_a_relative_location_is_resolved_against_the_database_directory(self):
        """The same rebasing open_doc does, not the process's cwd."""
        self._source("beside_the_db.bin")
        doc = self._doc("beside_the_db.bin", delete_original=0)

        self.db.add_docs([doc], validate=False)

        dest = os.path.join(self.db._file_dir(), "u-local")
        self.assertTrue(os.path.isfile(dest))

    def test_the_ingested_copy_is_what_exist_doc_and_open_doc_find(self):
        source = self._source()
        doc = self._doc(source)

        self.db.add_docs([doc], validate=False)

        exists, path = self.db.exist_doc(doc.id(), "filename1.ext")
        self.assertTrue(exists)
        self.assertEqual(path, os.path.join(self.db._file_dir(), "u-local"))

        file_obj = self.db.open_doc(doc.id(), "filename1.ext")
        file_obj.fopen()
        self.assertEqual(file_obj.fread(), b"payload")
        file_obj.fclose()

    # -- delete_original ----------------------------------------------------

    def test_delete_original_removes_the_source(self):
        source = self._source()
        doc = self._doc(source, delete_original=1)

        self.db.add_docs([doc], validate=False)

        self.assertFalse(os.path.exists(source), "the original should be deleted")
        self.assertTrue(os.path.isfile(os.path.join(self.db._file_dir(), "u-local")))

    def test_without_delete_original_the_source_survives(self):
        source = self._source()
        doc = self._doc(source, delete_original=0)

        self.db.add_docs([doc], validate=False)

        self.assertTrue(os.path.isfile(source))

    def test_a_failed_copy_does_not_delete_the_original(self):
        """MATLAB deletes only on the success branch, and so does this."""
        source = self._source()
        doc = self._doc(source, delete_original=1)

        # Make the destination unwritable by putting a directory in its place.
        os.makedirs(os.path.join(self.db._file_dir(), "u-local"), exist_ok=True)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False)

        self.assertTrue(os.path.isfile(source), "a failed ingest must not delete")
        self.assertEqual(self._row(doc.id())["cached_location"], "")
        self.assertTrue(any("Failed to ingest" in str(w.message) for w in caught))

    def test_a_missing_source_warns_and_still_adds_the_document(self):
        doc = self._doc(os.path.join(self._dir, "never-written.bin"))

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False)

        self.assertTrue(any("Failed to ingest" in str(w.message) for w in caught))
        self.assertEqual(self.db.get_docs(doc.id()).id(), doc.id())
        self.assertEqual(self._row(doc.id())["cached_location"], "")

    # -- not marked for ingestion -------------------------------------------

    def test_a_location_not_marked_for_ingestion_is_untouched(self):
        source = self._source()
        doc = self._doc(source, ingest=0, delete_original=1)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False)

        self.assertEqual(caught, [])
        self.assertTrue(os.path.isfile(source), "delete_original needs ingest")
        self.assertFalse(os.path.exists(os.path.join(self.db._file_dir(), "u-local")))
        self.assertEqual(self._row(doc.id())["cached_location"], "")

    # -- re-adding ----------------------------------------------------------

    def test_a_location_already_in_file_dir_is_not_copied_onto_itself(self):
        """The guard that keeps a re-add from deleting the only copy.

        A document whose location has been rewritten to its ingested path
        reaches _ingest_location with source == destination. Copying a file
        onto itself fails, and honoring delete_original there would delete
        the copy the database points at.
        """
        os.makedirs(self.db._file_dir(), exist_ok=True)
        already = os.path.join(self.db._file_dir(), "u-local")
        with open(already, "wb") as handle:
            handle.write(b"payload")

        doc = self._doc(already, delete_original=1)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False)

        self.assertTrue(os.path.isfile(already), "the only copy must survive")
        self.assertEqual(caught, [])
        self.assertEqual(self._row(doc.id())["cached_location"], already)


if __name__ == "__main__":
    unittest.main()
