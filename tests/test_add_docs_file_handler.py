"""add_docs(custom_file_handler=...): retrieval at add time.

Ports MATLAB's ``customFileHandler`` name-value argument to
``did.database/add_docs`` -> ``sqlitedb/do_add_doc``. A location marked
``ingest`` whose location is not a local path is handed to
``handler(dest_path, source_path)``, which must produce a local file there;
DID downloads nothing itself, in either language. The copy lands at
``<FileDir>/<uid>`` and is recorded in ``files.cached_location``, which is
where MATLAB's do_open_doc and check_exist_doc look for it.

The spelling is snake_case to match ``open_doc``'s parameter of the same
contract, and the failure paths follow MATLAB's do_add_doc: a warning and an
empty cached_location, not a raise -- the document is still added, its
orig_location is still recorded, and open_doc can retrieve the file later.
"""

import os
import sqlite3
import tempfile
import unittest
import warnings

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB

NDIC = "ndic://d-123/f-abc"


class TestAddDocsCustomFileHandler(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()
        self.db = SQLiteDB(os.path.join(self._dir, "t.sqlite"))
        self.db.add_branch("a")
        self.db.set_branch("a")

    def tearDown(self):
        self.db._close_db()

    def _doc_with_location(self, location, location_type, ingest=1, uid="u-remote"):
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", "placeholder")
        doc.add_file("filename2.ext", "placeholder")
        is_in, info, _ = doc.is_in_file_list("filename1.ext")
        self.assertTrue(is_in)
        info["locations"] = [
            {
                "location": location,
                "location_type": location_type,
                "uid": uid,
                "ingest": ingest,
                "delete_original": 0,
                "parameters": "",
            }
        ]
        return doc

    def _recording_handler(self, content=b"downloaded", produce=True):
        calls = []

        def handler(dest_path, source_path):
            calls.append((dest_path, source_path))
            if produce:
                with open(dest_path, "wb") as handle:
                    handle.write(content)

        return handler, calls

    def _files_row(self, doc_id, filename="filename1.ext"):
        cursor = self.db.dbid.cursor()
        row = cursor.execute(
            "SELECT f.uid, f.orig_location, f.cached_location, f.type "
            "FROM docs d, files f "
            "WHERE d.doc_id = ? AND f.doc_idx = d.doc_idx AND f.filename = ?",
            (doc_id, filename),
        ).fetchone()
        self.assertIsNotNone(row, "add_docs must record a files row")
        return row

    # -- the handler is used ------------------------------------------------

    def test_the_handler_retrieves_the_file_and_the_row_points_at_it(self):
        handler, calls = self._recording_handler()
        doc = self._doc_with_location(NDIC, "ndicloud")

        self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertEqual(len(calls), 1)
        dest_path, source_path = calls[0]
        self.assertEqual(source_path, NDIC)
        self.assertEqual(dest_path, os.path.join(self.db._file_dir(), "u-remote"))
        self.assertTrue(os.path.isfile(dest_path))

        row = self._files_row(doc.id())
        self.assertEqual(row["cached_location"], dest_path)
        self.assertEqual(row["orig_location"], NDIC)

    def test_the_ingested_file_is_what_exist_doc_and_open_doc_then_find(self):
        handler, _ = self._recording_handler(content=b"downloaded")
        doc = self._doc_with_location(NDIC, "ndicloud")

        self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        exists, file_path = self.db.exist_doc(doc.id(), "filename1.ext")
        self.assertTrue(exists, "an ingested file must exist for the document")
        self.assertEqual(file_path, os.path.join(self.db._file_dir(), "u-remote"))

        # And open_doc needs no handler now: the file is local.
        file_obj = self.db.open_doc(doc.id(), "filename1.ext")
        file_obj.fopen()
        self.assertEqual(file_obj.fread(), b"downloaded")
        file_obj.fclose()

    def test_a_stale_copy_is_not_passed_off_as_freshly_retrieved(self):
        os.makedirs(self.db._file_dir(), exist_ok=True)
        stale = os.path.join(self.db._file_dir(), "u-remote")
        with open(stale, "wb") as handle:
            handle.write(b"stale")

        handler, _ = self._recording_handler(produce=False)
        doc = self._doc_with_location(NDIC, "ndicloud")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertFalse(os.path.exists(stale))
        self.assertEqual(self._files_row(doc.id())["cached_location"], "")
        self.assertTrue(any("did not produce a file" in str(w.message) for w in caught))

    # -- when it is not used ------------------------------------------------

    def test_a_location_not_marked_for_ingestion_is_left_alone(self):
        """ingest defaults to 0 for url and ndicloud; nothing is fetched."""
        handler, calls = self._recording_handler()
        doc = self._doc_with_location(NDIC, "ndicloud", ingest=0)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertEqual(calls, [])
        self.assertEqual(caught, [])
        self.assertEqual(self._files_row(doc.id())["cached_location"], "")

    def test_a_local_location_does_not_go_through_the_handler(self):
        path = os.path.join(self._dir, "local.bin")
        with open(path, "wb") as handle:
            handle.write(b"payload")
        handler, calls = self._recording_handler()
        doc = self._doc_with_location(path, "file", uid="u-local")

        self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertEqual(calls, [], "a local file is not retrieved")
        # And the original is still there: delete_original is not honored.
        self.assertTrue(os.path.isfile(path))

    # -- failures warn, they do not lose the document -----------------------

    def test_no_handler_for_a_remote_ingest_warns_and_still_adds(self):
        """The negative case: not silently skipped, and not silently added."""
        doc = self._doc_with_location(NDIC, "ndicloud")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False)

        messages = [str(w.message) for w in caught]
        self.assertTrue(
            any("no custom_file_handler was supplied" in m for m in messages),
            f"expected a missing-handler warning, got {messages}",
        )
        self.assertTrue(
            any("DID does not download files itself" in m for m in messages),
            "the warning should say why, as open_doc's error does",
        )

        # MATLAB warns and carries on: the document is added with an empty
        # cached_location, so it can be retrieved later with a handler.
        self.assertEqual(self.db.get_docs(doc.id()).id(), doc.id())
        row = self._files_row(doc.id())
        self.assertEqual(row["cached_location"], "")
        self.assertEqual(row["orig_location"], NDIC)
        self.assertEqual(self.db.exist_doc(doc.id(), "filename1.ext"), (False, None))

    def test_a_raising_handler_warns_and_still_adds(self):
        def handler(dest_path, source_path):
            raise RuntimeError("cloud is down")

        doc = self._doc_with_location(NDIC, "ndicloud")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertTrue(any("cloud is down" in str(w.message) for w in caught))
        self.assertEqual(self.db.get_docs(doc.id()).id(), doc.id())
        self.assertEqual(self._files_row(doc.id())["cached_location"], "")

    def test_a_handler_that_produces_nothing_warns_and_still_adds(self):
        handler, calls = self._recording_handler(produce=False)
        doc = self._doc_with_location(NDIC, "ndicloud")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertEqual(len(calls), 1)
        self.assertTrue(any("did not produce a file" in str(w.message) for w in caught))
        self.assertEqual(self._files_row(doc.id())["cached_location"], "")

    def test_a_failed_ingest_does_not_roll_back_the_document(self):
        """A file that could not be fetched must not cost us the document."""

        def handler(dest_path, source_path):
            raise RuntimeError("cloud is down")

        doc = self._doc_with_location(NDIC, "ndicloud")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertIn(doc.id(), self.db.get_doc_ids("a"))

    # -- plumbing -----------------------------------------------------------

    def test_the_handler_reaches_the_implementation_through_add_docs(self):
        """add_docs -> _do_add_doc -> _populate_files, as MATLAB threads it."""
        handler, calls = self._recording_handler()
        doc = self._doc_with_location(NDIC, "ndicloud")

        self.db._do_add_doc(doc, "a", custom_file_handler=handler)

        self.assertEqual(len(calls), 1)

    def test_adding_the_same_document_to_a_second_branch_keeps_the_first_row(self):
        """INSERT OR IGNORE: the ingested copy recorded first is kept."""
        handler, calls = self._recording_handler()
        doc = self._doc_with_location(NDIC, "ndicloud")
        self.db.add_docs([doc], validate=False, custom_file_handler=handler)
        first = self._files_row(doc.id())["cached_location"]

        # A branch with no parent, so the document is not copied into it and
        # the add below is a genuine second add of the same document.
        self.db.add_branch("b", parent_branch_id="")
        self.db.add_docs(
            [doc], branch_id="b", validate=False, custom_file_handler=handler
        )

        self.assertEqual(len(calls), 2, "MATLAB re-runs its file loop per add")
        self.assertEqual(self._files_row(doc.id())["cached_location"], first)
        self.assertTrue(os.path.isfile(first))

    def test_the_files_table_still_has_one_row_per_location(self):
        """Ingestion changes what a row says, not how many rows there are."""
        handler, _ = self._recording_handler()
        doc = self._doc_with_location(NDIC, "ndicloud")
        self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        cursor = self.db.dbid.cursor()
        try:
            rows = cursor.execute(
                "SELECT filename, cached_location FROM files ORDER BY filename"
            ).fetchall()
        except sqlite3.Error as error:  # pragma: no cover - schema regression
            self.fail(f"files table unreadable: {error}")

        # The document carries two files: the retrieved one and the untouched
        # placeholder second file.
        self.assertEqual(
            [row["filename"] for row in rows], ["filename1.ext", "filename2.ext"]
        )
        self.assertTrue(rows[0]["cached_location"])
        self.assertEqual(rows[1]["cached_location"], "")


if __name__ == "__main__":
    unittest.main()
