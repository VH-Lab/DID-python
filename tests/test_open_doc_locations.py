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
