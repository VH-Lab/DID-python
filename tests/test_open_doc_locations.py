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
