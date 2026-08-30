import os
import shutil
import unittest

from did.document import Document
from did.file import ReadOnlyFileobj
from did.implementations.sqlitedb import SQLiteDB
from tests.helpers import make_doc_tree


def doc_uid(doc, filename="test_file.txt"):
    """The uid of a document's first location for `filename`."""
    is_in, info, _ = doc.is_in_file_list(filename)
    assert is_in
    locations = info["locations"]
    if isinstance(locations, dict):
        locations = [locations]
    return str(locations[0]["uid"])


class TestFileDocument(unittest.TestCase):
    DB_FILENAME = "test_file_document.sqlite"

    def setUp(self):
        # Create a temporary database for testing
        self.db_path = os.path.join(os.path.dirname(__file__), self.DB_FILENAME)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = SQLiteDB(self.db_path)
        self.db.add_branch("a")
        _, _, self.docs = make_doc_tree([1, 1, 1])
        for doc in self.docs:
            self.db._do_add_doc(doc, "a")

    def tearDown(self):
        # Clean up the database file, and the FileDir ingestion writes beside
        # it. Since local ingestion landed (#45) an added document's file is
        # copied to <db dir>/files/<uid>, so this test leaves artifacts in the
        # source tree unless they are removed here. Thirteen of them reached
        # main before this cleanup existed.
        self.db._close_db()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        shutil.rmtree(
            os.path.join(os.path.dirname(self.db_path), "files"), ignore_errors=True
        )

    def test_add_and_open_file(self):
        # Create a document of type 'demoFile'
        schema_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "src",
            "did",
            "example_schema",
            "demo_schema1",
        )
        Document.set_schema_path(schema_path)
        doc = Document("demoFile")

        # Create a dummy file to add. A relative location is resolved against
        # the database's directory (see test_path_rebasing), so the file has to
        # live there -- this previously wrote into the working directory and
        # the test still passed, because open_doc handed back a Fileobj over a
        # path that did not exist and the assertion only checked its type.
        dummy_file_path = os.path.join(os.path.dirname(self.db_path), "dummy_file.txt")
        with open(dummy_file_path, "w") as f:
            f.write("This is a test file.")

        # Add the file to the document. add_file defaults a local path to
        # ingest=1, delete_original=1.
        doc.add_file("test_file.txt", dummy_file_path)

        # Add the document to the database
        self.db._do_add_doc(doc, "a")

        # Ingestion moved it: the copy lives at <db dir>/files/<uid> and the
        # original is gone, exactly as MATLAB's do_add_doc leaves it.
        self.assertFalse(
            os.path.exists(dummy_file_path),
            "delete_original should have removed the original after ingestion",
        )
        exists, ingested = self.db.exist_doc(doc.id(), "test_file.txt")
        self.assertTrue(exists, "the ingested copy should be findable")
        self.assertEqual(ingested, os.path.join(self.db._file_dir(), doc_uid(doc)))

        # Open the file from the document
        file_obj = self.db.open_doc(doc.id(), "test_file.txt")
        self.assertIsInstance(file_obj, ReadOnlyFileobj)

        # Read it back, so the test fails if the file cannot actually be opened
        file_obj.fopen()
        self.assertIsNotNone(file_obj.fid, "the file could not be opened")
        data = file_obj.fread()
        file_obj.fclose()
        self.assertEqual(data, b"This is a test file.")


if __name__ == "__main__":
    unittest.main()
