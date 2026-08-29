"""open_doc resolving a document's file locations.

A document may list several locations for one file -- the shipped demoFile
template carries a local path and a URL for each -- and MATLAB tries them in
turn, returning the first it can reach. Three things were wrong here before
2026-08-28, all of them quiet:

* ``locations["location"]`` was read directly, so any MATLAB-written document
  (whose locations are a list) raised TypeError;
* a URL was joined onto the database directory, producing a path like
  ``/db/dir/https://host/file``;
* the resulting Fileobj was handed back unopened, because Fileobj.fopen()
  swallows the OSError, so a caller that did not check ``fid`` read b"" and
  saw no error.
"""

import os
import tempfile
import unittest
from unittest import mock

from did.database import FileAccessError
from did.document import Document
from did.file import ReadOnlyFileobj
from did.implementations.sqlitedb import SQLiteDB

URL = "https://example.org/data/thing.bin"


class TestOpenDocLocations(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()
        self.db = SQLiteDB(os.path.join(self._dir, "t.sqlite"))
        self.db.add_branch("a")
        self.db.set_branch("a")

    def tearDown(self):
        self.db._close_db()

    def _local_file(self, name="real.bin", content=b"payload"):
        path = os.path.join(self._dir, name)
        with open(path, "wb") as handle:
            handle.write(content)
        return path

    def _doc_with_locations(self, locations):
        """A demoFile document whose first file carries the given locations."""
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", "placeholder")
        doc.add_file("filename2.ext", "placeholder")
        is_in, info, _ = doc.is_in_file_list("filename1.ext")
        self.assertTrue(is_in)
        info["locations"] = locations
        return doc

    def test_list_of_locations_is_accepted(self):
        """The MATLAB shape: a list, local first."""
        path = self._local_file()
        doc = self._doc_with_locations(
            [
                {"location": path, "location_type": "file"},
                {"location": URL, "location_type": "url"},
            ]
        )
        self.db.add_docs([doc], validate=False)

        file_obj = self.db.open_doc(doc.id(), "filename1.ext")
        self.assertIsInstance(file_obj, ReadOnlyFileobj)
        file_obj.fopen()
        self.assertIsNotNone(file_obj.fid)
        self.assertEqual(file_obj.fread(), b"payload")
        file_obj.fclose()

    def test_reachable_location_is_used_even_if_listed_after_a_url(self):
        path = self._local_file()
        doc = self._doc_with_locations(
            [
                {"location": URL, "location_type": "url"},
                {"location": path, "location_type": "file"},
            ]
        )
        self.db.add_docs([doc], validate=False)

        file_obj = self.db.open_doc(doc.id(), "filename1.ext")
        file_obj.fopen()
        self.assertEqual(file_obj.fread(), b"payload")
        file_obj.fclose()

    def test_single_dict_location_still_works(self):
        path = self._local_file()
        doc = self._doc_with_locations({"location": path, "location_type": "file"})
        self.db.add_docs([doc], validate=False)
        self.assertIsInstance(
            self.db.open_doc(doc.id(), "filename1.ext"), ReadOnlyFileobj
        )

    def test_remote_only_raises_rather_than_manufacturing_a_path(self):
        doc = self._doc_with_locations([{"location": URL, "location_type": "url"}])
        self.db.add_docs([doc], validate=False)

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "filename1.ext")
        self.assertEqual(
            caught.exception.identifier,
            "DID:SQLITEDB:FileRetrieval:UnsupportedType",
        )
        self.assertNotIn(self._dir, str(caught.exception))

    def test_a_url_without_a_location_type_is_still_remote(self):
        doc = self._doc_with_locations([{"location": URL}])
        self.db.add_docs([doc], validate=False)
        with self.assertRaises(FileAccessError):
            self.db.open_doc(doc.id(), "filename1.ext")

    def test_missing_local_file_raises(self):
        doc = self._doc_with_locations(
            [{"location": "/no/such/file.bin", "location_type": "file"}]
        )
        self.db.add_docs([doc], validate=False)

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "filename1.ext")
        self.assertEqual(caught.exception.identifier, "DID:SQLITEDB:open")

    def test_error_is_still_a_filenotfounderror(self):
        """Callers written against the older behavior keep working."""
        doc = self._doc_with_locations([{"location": URL, "location_type": "url"}])
        self.db.add_docs([doc], validate=False)
        with self.assertRaises(FileNotFoundError):
            self.db.open_doc(doc.id(), "filename1.ext")

    def test_file_not_in_the_document_still_raises(self):
        doc = self._doc_with_locations([{"location": self._local_file()}])
        self.db.add_docs([doc], validate=False)
        with self.assertRaises(FileNotFoundError):
            self.db.open_doc(doc.id(), "not_listed.ext")


class TestCustomFileHandler(TestOpenDocLocations):
    """Retrieval is supplied by the caller, not by DID.

    Mirrors MATLAB's customFileHandler name-value argument to do_open_doc.
    NDI-matlab uses it to resolve ndic://<datasetId>/<fileUid> by minting a
    pre-signed URL and downloading it; DID downloads nothing itself, in either
    language. Only ndic:// is supported for now -- plain URLs are deliberately
    not handled by DID.
    """

    NDIC = "ndic://d-123/f-abc"

    def _recording_handler(self, content=b"downloaded", produce=True):
        calls = []

        def handler(dest_path, source_path):
            calls.append((dest_path, source_path))
            if produce:
                with open(dest_path, "wb") as handle:
                    handle.write(content)

        return handler, calls

    def test_handler_is_called_and_its_file_is_returned(self):
        doc = self._doc_with_locations(
            [{"location": self.NDIC, "location_type": "ndicloud", "uid": "f-abc"}]
        )
        self.db.add_docs([doc], validate=False)
        handler, calls = self._recording_handler()

        file_obj = self.db.open_doc(
            doc.id(), "filename1.ext", custom_file_handler=handler
        )
        self.assertEqual(len(calls), 1)
        dest_path, source_path = calls[0]
        self.assertEqual(source_path, self.NDIC)
        self.assertTrue(dest_path.endswith("f-abc"))

        file_obj.fopen()
        self.assertEqual(file_obj.fread(), b"downloaded")
        file_obj.fclose()

    def test_ndic_is_remote_even_without_a_location_type(self):
        doc = self._doc_with_locations([{"location": self.NDIC}])
        self.db.add_docs([doc], validate=False)
        handler, calls = self._recording_handler()

        self.db.open_doc(doc.id(), "filename1.ext", custom_file_handler=handler)
        self.assertEqual(len(calls), 1)

    def test_a_local_file_is_preferred_and_the_handler_is_not_called(self):
        path = self._local_file()
        doc = self._doc_with_locations(
            [
                {"location": self.NDIC, "location_type": "ndicloud"},
                {"location": path, "location_type": "file"},
            ]
        )
        self.db.add_docs([doc], validate=False)
        handler, calls = self._recording_handler()

        file_obj = self.db.open_doc(
            doc.id(), "filename1.ext", custom_file_handler=handler
        )
        self.assertEqual(calls, [], "a local file should not be re-downloaded")
        file_obj.fopen()
        self.assertEqual(file_obj.fread(), b"payload")
        file_obj.fclose()

    def test_a_raising_handler_becomes_a_FileAccessError(self):
        doc = self._doc_with_locations(
            [{"location": self.NDIC, "location_type": "ndicloud"}]
        )
        self.db.add_docs([doc], validate=False)

        def handler(dest_path, source_path):
            raise RuntimeError("cloud is down")

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "filename1.ext", custom_file_handler=handler)
        self.assertEqual(
            caught.exception.identifier,
            "DID:SQLITEDB:FileRetrieval:CustomHandlerFailed",
        )
        self.assertIn("cloud is down", str(caught.exception))

    def test_a_handler_that_produces_nothing_is_an_error(self):
        """MATLAB checks isfile(destPath) after the handler returns."""
        doc = self._doc_with_locations(
            [{"location": self.NDIC, "location_type": "ndicloud"}]
        )
        self.db.add_docs([doc], validate=False)
        handler, _ = self._recording_handler(produce=False)

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "filename1.ext", custom_file_handler=handler)
        self.assertEqual(
            caught.exception.identifier,
            "DID:SQLITEDB:FileRetrieval:CustomHandlerFailed",
        )
        self.assertIn("did not produce a file", str(caught.exception))

    def test_without_a_handler_a_remote_location_is_unsupported(self):
        doc = self._doc_with_locations(
            [{"location": self.NDIC, "location_type": "ndicloud"}]
        )
        self.db.add_docs([doc], validate=False)

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "filename1.ext")
        self.assertEqual(
            caught.exception.identifier,
            "DID:SQLITEDB:FileRetrieval:UnsupportedType",
        )

    def test_a_second_location_is_tried_when_the_first_fails(self):
        doc = self._doc_with_locations(
            [
                {"location": "ndic://d-123/gone", "location_type": "ndicloud"},
                {"location": self.NDIC, "location_type": "ndicloud", "uid": "f-abc"},
            ]
        )
        self.db.add_docs([doc], validate=False)

        def handler(dest_path, source_path):
            if source_path.endswith("gone"):
                raise RuntimeError("404")
            with open(dest_path, "wb") as handle:
                handle.write(b"second")

        file_obj = self.db.open_doc(
            doc.id(), "filename1.ext", custom_file_handler=handler
        )
        file_obj.fopen()
        self.assertEqual(file_obj.fread(), b"second")
        file_obj.fclose()

    def test_a_stale_download_is_not_served_as_fresh(self):
        """temppath persists, so an earlier download must not stand in.

        Caught by the suite: uids are unique per document, not globally, so a
        leftover file at the same destination made a handler that produced
        nothing look like it had succeeded -- and would have served one
        document's bytes for another.
        """
        from did.common import PathConstants

        doc = self._doc_with_locations(
            [{"location": self.NDIC, "location_type": "ndicloud", "uid": "f-abc"}]
        )
        self.db.add_docs([doc], validate=False)

        stale = os.path.join(PathConstants().temppath, "f-abc")
        with open(stale, "wb") as handle:
            handle.write(b"stale bytes from an earlier download")
        self.addCleanup(lambda: os.path.exists(stale) and os.remove(stale))

        handler, _ = self._recording_handler(produce=False)
        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "filename1.ext", custom_file_handler=handler)
        self.assertEqual(
            caught.exception.identifier,
            "DID:SQLITEDB:FileRetrieval:CustomHandlerFailed",
        )

    def test_a_fresh_download_replaces_a_stale_one(self):
        from did.common import PathConstants

        doc = self._doc_with_locations(
            [{"location": self.NDIC, "location_type": "ndicloud", "uid": "f-abc"}]
        )
        self.db.add_docs([doc], validate=False)

        stale = os.path.join(PathConstants().temppath, "f-abc")
        with open(stale, "wb") as handle:
            handle.write(b"stale")
        self.addCleanup(lambda: os.path.exists(stale) and os.remove(stale))

        handler, _ = self._recording_handler(content=b"fresh")
        file_obj = self.db.open_doc(
            doc.id(), "filename1.ext", custom_file_handler=handler
        )
        file_obj.fopen()
        self.assertEqual(file_obj.fread(), b"fresh")
        file_obj.fclose()


class TestMatlabShapedDatabase(TestOpenDocLocations):
    """Reading a database written the way MATLAB writes one.

    MATLAB ingests a local file into its FileDir -- `files/` beside the .sqlite
    file -- names the copy by the location's uid, and then deletes the original.
    The document JSON still holds the original path, which no longer exists, so
    the file is findable only through the `files` table.

    This is the half of the cross-language gap that can be verified without
    MATLAB: it builds that exact shape by hand and checks open_doc resolves it.
    """

    def _matlab_shaped_doc(self, content=b"ingested payload"):
        """Add a document, then rearrange it into MATLAB's on-disk shape."""
        original = self._local_file("original.bin", content)
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", original)
        doc.add_file("filename2.ext", original)
        self.db.add_docs([doc])

        # The uid MATLAB would have named the ingested copy by.
        cursor = self.db.dbid.cursor()
        row = cursor.execute(
            "SELECT f.uid FROM docs d, files f "
            "WHERE d.doc_idx = f.doc_idx AND d.doc_id = ? AND f.filename = ?",
            (doc.id(), "filename1.ext"),
        ).fetchone()
        self.assertIsNotNone(row, "the files table should carry a row per file")
        uid = row["uid"]

        # Ingest it: copy to <db dir>/files/<uid>, then delete the original.
        file_dir = os.path.join(os.path.dirname(self.db.connection), "files")
        os.makedirs(file_dir, exist_ok=True)
        with open(os.path.join(file_dir, uid), "wb") as handle:
            handle.write(content)
        os.remove(original)

        return doc, original

    def test_ingested_copy_is_found_after_the_original_is_deleted(self):
        doc, original = self._matlab_shaped_doc()
        self.assertFalse(os.path.exists(original), "fixture should have removed it")

        file_obj = self.db.open_doc(doc.id(), "filename1.ext")
        file_obj.fopen()
        self.assertIsNotNone(
            file_obj.fid,
            "an ingested file must resolve through the files table; the "
            "document JSON's location no longer exists",
        )
        self.assertEqual(file_obj.fread(), b"ingested payload")
        file_obj.fclose()

    def test_a_file_with_no_ingested_copy_and_no_original_is_an_error(self):
        doc, _ = self._matlab_shaped_doc()
        # filename2.ext shares the deleted original but was never ingested.
        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "filename2.ext")
        self.assertEqual(caught.exception.identifier, "DID:SQLITEDB:open")

    def test_the_files_table_is_what_makes_it_work(self):
        """Delete the rows and the same document becomes unreadable."""
        doc, _ = self._matlab_shaped_doc()
        cursor = self.db.dbid.cursor()
        cursor.execute(
            "DELETE FROM files WHERE doc_idx IN "
            "(SELECT doc_idx FROM docs WHERE doc_id = ?)",
            (doc.id(),),
        )
        self.db.dbid.commit()

        with self.assertRaises(FileAccessError):
            self.db.open_doc(doc.id(), "filename1.ext")


class TestFileCacheOnOpen(TestOpenDocLocations):
    """A retrieved file goes into the file cache, and is served from it next time.

    This is MATLAB's do_open_doc behaviour: it builds its candidate paths as
    `<filecachepath>/<uid>` before `<FileDir>/<uid>`, and after a successful
    retrieval it calls addFile(destPath, uid) and returns the cached copy.
    Without it every remote file is fetched again on every open.
    """

    NDIC = "ndic://d-123/f-abc"
    UID = "f" * 33  # the length of a did unique id, which is what the cache indexes

    def _remote_doc(self):
        doc = self._doc_with_locations(
            [{"location": self.NDIC, "location_type": "ndicloud", "uid": self.UID}]
        )
        self.db.add_docs([doc], validate=False)
        return doc

    def _handler(self, content=b"downloaded", fail=False):
        calls = []

        def handler(dest_path, source_path):
            calls.append(source_path)
            if fail:
                raise OSError("the network is gone")
            with open(dest_path, "wb") as handle:
                handle.write(content)

        return handler, calls

    def _cache(self):
        from did.common import get_cache

        return get_cache()

    def test_a_retrieved_file_lands_in_the_cache(self):
        doc = self._remote_doc()
        handler, calls = self._handler()

        file_obj = self.db.open_doc(
            doc.id(), "filename1.ext", custom_file_handler=handler
        )
        self.assertEqual(len(calls), 1)

        cache = self._cache()
        self.assertTrue(cache.is_file(self.UID))
        self.assertEqual(
            os.path.abspath(file_obj.fullpathfilename),
            os.path.abspath(cache.full_path(self.UID)),
            "the returned file should be the cached copy, not the temp one",
        )

    def test_the_second_open_does_not_retrieve_again(self):
        doc = self._remote_doc()
        handler, _ = self._handler()
        self.db.open_doc(doc.id(), "filename1.ext", custom_file_handler=handler)

        # A handler that would fail if it were called at all. The open still
        # succeeds, which it can only do from the cache.
        failing, failing_calls = self._handler(fail=True)
        file_obj = self.db.open_doc(
            doc.id(), "filename1.ext", custom_file_handler=failing
        )
        self.assertEqual(failing_calls, [], "the cached copy should have been used")

        file_obj.fopen()
        self.assertEqual(file_obj.fread(), b"downloaded")
        file_obj.fclose()

    def test_reading_from_the_cache_marks_the_file_as_used(self):
        doc = self._remote_doc()
        handler, _ = self._handler()
        self.db.open_doc(doc.id(), "filename1.ext", custom_file_handler=handler)

        cache = self._cache()
        before = cache.file_list()[2][0]
        self.db.open_doc(doc.id(), "filename1.ext", custom_file_handler=handler)
        after = cache.file_list()[2][0]
        self.assertGreaterEqual(
            after,
            before,
            "an open that hits the cache must touch the entry, or eviction "
            "would drop exactly the files that are read most",
        )

    def test_clearing_the_cache_sends_the_next_open_back_to_the_handler(self):
        doc = self._remote_doc()
        handler, calls = self._handler()
        self.db.open_doc(doc.id(), "filename1.ext", custom_file_handler=handler)
        self._cache().clear()

        self.db.open_doc(doc.id(), "filename1.ext", custom_file_handler=handler)
        self.assertEqual(len(calls), 2)

    def test_an_unusable_cache_does_not_break_the_open(self):
        """The cache is an optimization; losing it costs a re-fetch, not the file."""
        doc = self._remote_doc()
        handler, _ = self._handler()

        with mock.patch.object(SQLiteDB, "_file_cache", staticmethod(lambda: None)):
            file_obj = self.db.open_doc(
                doc.id(), "filename1.ext", custom_file_handler=handler
            )
            file_obj.fopen()
            self.assertEqual(file_obj.fread(), b"downloaded")
            file_obj.fclose()

        # And the cache is back afterwards, so nothing leaks into other tests.
        self.assertIsNotNone(SQLiteDB._file_cache())
