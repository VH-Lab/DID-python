"""exist_doc: is a document's file on disk, and where?

Ports MATLAB's ``did.database/exist_doc`` -> ``sqlitedb/check_exist_doc``,
whose two outputs ``[tf, file_path]`` arrive here as a tuple. MATLAB builds
its candidate paths from the same ``docs, files`` join ``do_open_doc`` uses,
so the two can never disagree about where a file is; these tests hold Python
to the same standard by checking exist_doc against open_doc directly.
"""

import os
import tempfile
import unittest

from did.database import FileAccessError
from did.document import Document
from did.implementations.sqlitedb import SQLiteDB

NDIC = "ndic://d-123/f-abc"
URL = "https://example.org/data/thing.bin"


class TestExistDoc(unittest.TestCase):
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
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", "placeholder")
        doc.add_file("filename2.ext", "placeholder")
        is_in, info, _ = doc.is_in_file_list("filename1.ext")
        self.assertTrue(is_in)
        info["locations"] = locations
        return doc

    def test_an_existing_local_file_is_reported_with_its_path(self):
        path = self._local_file()
        doc = self._doc_with_locations([{"location": path, "location_type": "file"}])
        self.db.add_docs([doc], validate=False)

        exists, file_path = self.db.exist_doc(doc.id(), "filename1.ext")
        self.assertTrue(exists)
        self.assertEqual(file_path, os.path.abspath(path))

    def test_a_listed_but_absent_file_does_not_exist(self):
        """A `files` row is not proof of a file: MATLAB checks isfile too."""
        missing = os.path.join(self._dir, "never-written.bin")
        doc = self._doc_with_locations([{"location": missing, "location_type": "file"}])
        self.db.add_docs([doc], validate=False)

        self.assertEqual(self.db.exist_doc(doc.id(), "filename1.ext"), (False, None))

    def test_a_remote_only_file_does_not_exist_locally(self):
        doc = self._doc_with_locations(
            [{"location": NDIC, "location_type": "ndicloud"}]
        )
        self.db.add_docs([doc], validate=False)

        self.assertEqual(self.db.exist_doc(doc.id(), "filename1.ext"), (False, None))

    def test_an_unknown_document_is_false_rather_than_an_error(self):
        """MATLAB's join returns no rows and tf is false; it does not throw."""
        self.assertEqual(
            self.db.exist_doc("no_such_doc_id", "filename1.ext"), (False, None)
        )

    def test_a_filename_the_document_does_not_list_is_false(self):
        path = self._local_file()
        doc = self._doc_with_locations([{"location": path, "location_type": "file"}])
        self.db.add_docs([doc], validate=False)

        self.assertEqual(self.db.exist_doc(doc.id(), "not_a_file.ext"), (False, None))

    def test_a_document_object_is_accepted_in_place_of_an_id(self):
        """MATLAB: "DOCUMENT_ID can be ... a did.document object itself"."""
        path = self._local_file()
        doc = self._doc_with_locations([{"location": path, "location_type": "file"}])
        self.db.add_docs([doc], validate=False)

        self.assertEqual(
            self.db.exist_doc(doc, "filename1.ext"),
            self.db.exist_doc(doc.id(), "filename1.ext"),
        )

    def test_the_filename_is_mandatory(self):
        """MATLAB errors DID:SQLITEDB:open when filename is empty."""
        path = self._local_file()
        doc = self._doc_with_locations([{"location": path, "location_type": "file"}])
        self.db.add_docs([doc], validate=False)

        for empty in ("", "   ", None):
            with self.assertRaises(ValueError):
                self.db.exist_doc(doc.id(), empty)

    def test_the_first_reachable_location_wins(self):
        """MATLAB: "only the file path for the first document is returned"."""
        path = self._local_file()
        doc = self._doc_with_locations(
            [
                {"location": URL, "location_type": "url"},
                {"location": path, "location_type": "file"},
                {"location": self._local_file("second.bin"), "location_type": "file"},
            ]
        )
        self.db.add_docs([doc], validate=False)

        exists, file_path = self.db.exist_doc(doc.id(), "filename1.ext")
        self.assertTrue(exists)
        self.assertEqual(file_path, os.path.abspath(path))

    def test_a_relative_location_comes_back_absolute(self):
        """MATLAB documents FILE_PATH as absolute."""
        self._local_file("beside_the_db.bin")
        doc = self._doc_with_locations(
            [{"location": "beside_the_db.bin", "location_type": "file"}]
        )
        self.db.add_docs([doc], validate=False)

        exists, file_path = self.db.exist_doc(doc.id(), "filename1.ext")
        self.assertTrue(exists)
        self.assertTrue(os.path.isabs(file_path))
        self.assertTrue(os.path.isfile(file_path))

    def test_an_ingested_copy_is_found_after_the_original_is_deleted(self):
        """MATLAB looks under <FileDir>/<uid>, which is all an ingest leaves."""
        doc = self._doc_with_locations(
            [{"location": "/gone/original.bin", "location_type": "file", "uid": "u-1"}]
        )
        self.db.add_docs([doc], validate=False)
        self.assertEqual(self.db.exist_doc(doc.id(), "filename1.ext"), (False, None))

        file_dir = self.db._file_dir()
        os.makedirs(file_dir, exist_ok=True)
        ingested = os.path.join(file_dir, "u-1")
        with open(ingested, "wb") as handle:
            handle.write(b"ingested payload")

        exists, file_path = self.db.exist_doc(doc.id(), "filename1.ext")
        self.assertTrue(exists)
        self.assertEqual(file_path, os.path.abspath(ingested))


class TestExistDocAgreesWithOpenDoc(TestExistDoc):
    """exist_doc is true exactly when open_doc succeeds with no handler.

    The two share one resolution path precisely so this holds; MATLAB's
    check_exist_doc searches the same candidate roots as do_open_doc for the
    same reason.
    """

    def _open_succeeds(self, doc_id, filename):
        try:
            file_obj = self.db.open_doc(doc_id, filename)
        except (FileAccessError, FileNotFoundError, ValueError):
            return False
        file_obj.fopen()
        opened = file_obj.fid is not None
        file_obj.fclose()
        return opened

    def _assert_agree(self, doc, filename="filename1.ext"):
        exists, _ = self.db.exist_doc(doc.id(), filename)
        self.assertEqual(exists, self._open_succeeds(doc.id(), filename))

    def test_they_agree_on_a_present_file(self):
        doc = self._doc_with_locations(
            [{"location": self._local_file(), "location_type": "file"}]
        )
        self.db.add_docs([doc], validate=False)
        self._assert_agree(doc)

    def test_they_agree_on_an_absent_file(self):
        doc = self._doc_with_locations(
            [{"location": os.path.join(self._dir, "gone.bin"), "location_type": "file"}]
        )
        self.db.add_docs([doc], validate=False)
        self._assert_agree(doc)

    def test_they_agree_on_a_remote_only_file(self):
        doc = self._doc_with_locations(
            [{"location": NDIC, "location_type": "ndicloud"}]
        )
        self.db.add_docs([doc], validate=False)
        self._assert_agree(doc)

    def test_the_path_exist_doc_reports_is_the_one_open_doc_reads(self):
        path = self._local_file(content=b"payload")
        doc = self._doc_with_locations(
            [
                {"location": URL, "location_type": "url"},
                {"location": path, "location_type": "file"},
            ]
        )
        self.db.add_docs([doc], validate=False)

        _, file_path = self.db.exist_doc(doc.id(), "filename1.ext")
        file_obj = self.db.open_doc(doc.id(), "filename1.ext")
        file_obj.fopen()
        contents = file_obj.fread()
        file_obj.fclose()
        with open(file_path, "rb") as handle:
            self.assertEqual(handle.read(), contents)


if __name__ == "__main__":
    unittest.main()
