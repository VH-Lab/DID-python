"""Directory-traversal guards on ingest and open_doc.

Regression test for DID-python issue #58: a document's `file_list[i].
locations[j].uid` and `location` are attacker-controlled when the document
is pulled from a cloud store, and both used to reach the filesystem
verbatim -- `os.path.join(file_dir, uid)` on the write side, and
`_resolve_local(location)` (which returned an absolute location verbatim
and joined a relative one with ``..`` intact) on the read side.

The behaviour to pin is the "refuse, do not substitute" rule ported from
ndi-python's ``TestGetBinaryPathTraversal``:

* an unsafe uid (path separator, ``.``, ``..``, empty basename) is refused
  at ingest -- the document is not partly written;
* an unsafe location (``..`` that escapes the db dir, or an absolute path
  outside it) is refused at ingest for the same reason;
* on the read side, a row written by an older, unguarded DID that carries
  such a value is filtered out -- ``open_doc`` cannot be steered into
  reading a file outside the database directory through it.
"""

import os
import tempfile
import unittest

from did.database import FileAccessError
from did.document import Document
from did.implementations.sqlitedb import SQLiteDB


class _TraversalTestBase(unittest.TestCase):
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

    def _doc(self, uid, location, ingest=1, delete_original=0):
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


class TestUnsafeUidRefusedAtIngest(_TraversalTestBase):
    """A crafted uid must not steer the ingest write outside FileDir."""

    def test_uid_with_parent_traversal_is_refused(self):
        source = self._source()
        doc = self._doc(uid="../../.ssh/authorized_keys", location=source)

        with self.assertRaises(ValueError) as caught:
            self.db.add_docs([doc], validate=False)
        self.assertIn("issue #58", str(caught.exception))

    def test_uid_with_a_path_separator_is_refused(self):
        source = self._source()
        doc = self._doc(uid="a/b", location=source)
        with self.assertRaises(ValueError):
            self.db.add_docs([doc], validate=False)

    def test_uid_that_is_a_dot_segment_is_refused(self):
        source = self._source()
        for bad in (".", ".."):
            doc = self._doc(uid=bad, location=source)
            with self.assertRaises(ValueError):
                self.db.add_docs([doc], validate=False)

    def test_uid_with_a_null_byte_is_refused(self):
        source = self._source()
        doc = self._doc(uid="ok\x00.txt", location=source)
        with self.assertRaises(ValueError):
            self.db.add_docs([doc], validate=False)

    def test_a_refused_uid_leaves_no_document_in_the_database(self):
        """Refuse, do not substitute -- and do not write half of it."""
        source = self._source()
        doc = self._doc(uid="../evil", location=source)
        with self.assertRaises(ValueError):
            self.db.add_docs([doc], validate=False)

        cursor = self.db.dbid.cursor()
        row = cursor.execute(
            "SELECT doc_id FROM docs WHERE doc_id = ?", (doc.id(),)
        ).fetchone()
        self.assertIsNone(row, "a refused ingest must not leave the document behind")

    def test_a_refused_uid_does_not_write_the_dest_file(self):
        """Concretely: no file appears at the traversal target."""
        source = self._source(name="src.bin")
        outside = os.path.abspath(
            os.path.join(self._dir, os.pardir, "outside-file.bin")
        )
        self.addCleanup(lambda: os.path.exists(outside) and os.remove(outside))

        # ../<basename> lands next to db_dir -- a real filesystem escape.
        doc = self._doc(uid="../outside-file.bin", location=source)
        with self.assertRaises(ValueError):
            self.db.add_docs([doc], validate=False)
        self.assertFalse(
            os.path.exists(outside),
            "the traversal target must not have been written",
        )


class TestUnsafeLocationRefusedAtIngest(_TraversalTestBase):
    def test_relative_location_that_escapes_db_dir_is_refused(self):
        doc = self._doc(uid="u-1", location="../../etc/passwd")
        with self.assertRaises(ValueError) as caught:
            self.db.add_docs([doc], validate=False)
        self.assertIn("issue #58", str(caught.exception))

    def test_absolute_location_outside_db_dir_is_refused(self):
        # /etc/hostname is readable on Linux; the point is only that it is
        # outside the database directory, so the ingest source path check
        # rejects it before the copy ever runs.
        doc = self._doc(uid="u-1", location="/etc/hostname")
        with self.assertRaises(ValueError) as caught:
            self.db.add_docs([doc], validate=False)
        self.assertIn("issue #58", str(caught.exception))


class TestOpenDocFiltersUnguardedRows(_TraversalTestBase):
    """Defense in depth: rows written before the guard cannot steer open_doc."""

    def _document_with_files_row(self, uid, orig_location, cached_location=""):
        """Insert a row bypassing ingest -- the way an older DID would have."""
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", "placeholder", ingest=0, delete_original=0)
        self.db.add_docs([doc], validate=False)

        cursor = self.db.dbid.cursor()
        doc_idx_row = cursor.execute(
            "SELECT doc_idx FROM docs WHERE doc_id = ?", (doc.id(),)
        ).fetchone()
        self.assertIsNotNone(doc_idx_row)
        cursor.execute(
            "INSERT OR REPLACE INTO files "
            "(doc_idx, filename, uid, orig_location, cached_location, "
            "type, parameters) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                doc_idx_row["doc_idx"],
                "filename1.ext",
                uid,
                orig_location,
                cached_location,
                "file",
                "",
            ),
        )
        self.db.dbid.commit()
        return doc

    def test_open_doc_does_not_read_a_file_via_a_traversal_orig_location(self):
        # An attacker-planted orig_location pointing at /etc/hostname.
        doc = self._document_with_files_row(
            uid="u-plain",
            orig_location="/etc/hostname",
        )
        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "filename1.ext")
        self.assertEqual(caught.exception.identifier, "DID:SQLITEDB:open")

    def test_open_doc_ignores_a_relative_orig_location_that_escapes(self):
        doc = self._document_with_files_row(
            uid="u-plain",
            orig_location="../../etc/hostname",
        )
        with self.assertRaises(FileAccessError):
            self.db.open_doc(doc.id(), "filename1.ext")

    def test_open_doc_does_not_open_via_a_bad_uid_from_the_files_table(self):
        # An unsafe uid from a legacy row -- the FileDir-derived and cache-
        # derived candidates must not be built from it.
        outside = os.path.abspath(
            os.path.join(self._dir, os.pardir, "outside-legacy.bin")
        )
        with open(outside, "wb") as handle:
            handle.write(b"attacker-planted")
        self.addCleanup(lambda: os.path.exists(outside) and os.remove(outside))

        # An uid that escapes FileDir back into the parent, landing at the
        # attacker-planted file. FileDir is <db_dir>/files, so ..<basename>
        # from FileDir reaches <db_dir>/outside-legacy.bin -- we point at
        # a real file that lives one level up from db_dir instead.
        doc = self._document_with_files_row(
            uid="../../outside-legacy.bin",
            orig_location="",
        )
        with self.assertRaises(FileAccessError):
            self.db.open_doc(doc.id(), "filename1.ext")


if __name__ == "__main__":
    unittest.main()
