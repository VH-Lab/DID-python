"""add_docs storing a document that carries a file series.

Ports the add-side half of DID-matlab's ``TestFileSeriesRoundTrip.m``. See
VH-Lab/DID-matlab#173 and #185. The read side -- ``open_doc`` on a member --
is a separate file.

Two things happen when a document with a series reaches ``do_add_doc``, and
neither had a test:

* ``_reject_series_without_ingest_locations`` refuses a document whose series
  declares present members but records no ``ingest_locations``. That is the
  shape a document has after a round trip through storage, where nothing
  could fill the manifest's uids -- and the member loop would skip them in
  silence, because an empty list is simply zero passes. It is a hard raise
  out of ``add_docs``, so a false positive bricks a caller and a false
  negative stores half a series; both directions are pinned here.
* ``_ingest_series_members`` copies each member into ``<FileDir>/<uid>``.
  Members carry no ``files`` row -- resolution goes through the manifest --
  so the absence of those rows is part of the contract, not an omission.

MATLAB raises identified errors (``DID:SQLITEDB:FileSeries:MembersNotLocatable``);
Python's sqlitedb raises ``ValueError`` on the add path, as the surrounding
add_docs guards already do, so the refusals are matched on the input refused
and on the message, not on an identifier.
"""

import os
import tempfile
import unittest
import warnings

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB

SERIES = "chunkdata.bin"
REMOTE = "ndic://d-123/f-abc"


class SeriesDatabaseTestCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()
        self.db = SQLiteDB(os.path.join(self._dir, "t.sqlite"))
        self.db.add_branch("a")
        self.db.set_branch("a")
        self._dbs = [self.db]

    def tearDown(self):
        for db in self._dbs:
            db._close_db()

    def other_db(self, name="other"):
        """A second database, in its own directory.

        Its own directory matters: FileDir is ``files/`` beside the database
        file, so two databases sharing a directory share their ingested
        copies -- and a test that then asked what the second one had written
        would be reading the first one's work.
        """
        directory = os.path.join(self._dir, name)
        os.makedirs(directory, exist_ok=True)
        db = SQLiteDB(os.path.join(directory, "db.sqlite"))
        db.add_branch("a")
        db.set_branch("a")
        self._dbs.append(db)
        return db

    def write_member(self, name, content=None):
        store = os.path.join(self._dir, "store")
        os.makedirs(store, exist_ok=True)
        path = os.path.join(store, name)
        with open(path, "wb") as handle:
            handle.write(content if content is not None else name.encode() * 4)
        return path

    def series_doc(self, locations, **kwargs):
        doc = Document("demoSeries", **{"demoSeries.value": 1})
        doc.add_file_series(SERIES, locations, **kwargs)
        return doc

    def member_uids(self, doc):
        return [e["uid"] for e in doc.series_ingest_locations(SERIES)]

    def files_rows(self):
        cursor = self.db.dbid.cursor()
        return [
            (row["filename"], row["uid"])
            for row in cursor.execute("SELECT filename, uid FROM files").fetchall()
        ]

    def file_dir_names(self, db=None):
        db = db or self.db
        directory = db._file_dir()
        return sorted(os.listdir(directory)) if os.path.isdir(directory) else []


class TestSeriesMemberIngestion(SeriesDatabaseTestCase):
    def test_members_land_under_their_manifest_uid(self):
        """<FileDir>/<uid> is where every later lookup goes: the manifest
        gives a slot's uid, and cached_path_for_uid resolves the uid. If the
        copy landed anywhere else nothing would ever find it."""
        doc = self.series_doc(
            [self.write_member(n) for n in ("a", "b", "c")], delete_original=0
        )
        uids = self.member_uids(doc)

        self.db.add_docs([doc], validate=False)

        for uid in uids:
            self.assertTrue(os.path.isfile(os.path.join(self.db._file_dir(), uid)))

    def test_each_member_keeps_its_own_bytes(self):
        contents = [b"first", b"second!!", b"third"]
        locations = [
            self.write_member(name, content)
            for name, content in zip("abc", contents, strict=True)
        ]
        doc = self.series_doc(locations, delete_original=0)
        uids = self.member_uids(doc)

        self.db.add_docs([doc], validate=False)

        for uid, expected in zip(uids, contents, strict=True):
            with open(os.path.join(self.db._file_dir(), uid), "rb") as handle:
                self.assertEqual(handle.read(), expected)

    def test_members_get_no_files_table_row(self):
        """The point of the whole design. A member's resolution goes through
        the manifest, so 28,000 members add 28,000 files rows' worth of
        nothing. Only the manifest -- an ordinary file -- gets a row."""
        doc = self.series_doc(
            [self.write_member(n) for n in ("a", "b", "c")], delete_original=0
        )

        self.db.add_docs([doc], validate=False)

        rows = self.files_rows()
        self.assertEqual([name for name, _uid in rows], [SERIES])
        self.assertEqual(rows[0][1], doc.file_uids(SERIES)[0])

    def test_a_sparse_series_ingests_only_the_present_members(self):
        doc = self.series_doc(
            [self.write_member(n) for n in ("a", "b")],
            indices=[1, 5],
            delete_original=0,
        )
        uids = self.member_uids(doc)

        self.db.add_docs([doc], validate=False)

        # Two members plus the manifest; the three empty slots cost nothing.
        self.assertEqual(len(self.file_dir_names()), 3)
        for uid in uids:
            self.assertIn(uid, self.file_dir_names())

    def test_delete_original_removes_the_member_sources(self):
        """A series follows add_file: a local member's original goes on
        ingest unless the caller says otherwise. 28,000 sources left behind
        is the same disk twice."""
        locations = [self.write_member(n) for n in ("a", "b")]
        doc = self.series_doc(locations)  # default for a local file: delete

        self.db.add_docs([doc], validate=False)

        for location in locations:
            self.assertFalse(os.path.exists(location))
        self.assertTrue(self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0])

    def test_keeping_the_originals_leaves_them_alone(self):
        locations = [self.write_member(n) for n in ("a", "b")]
        doc = self.series_doc(locations, delete_original=0)

        self.db.add_docs([doc], validate=False)

        for location in locations:
            self.assertTrue(os.path.isfile(location))
        self.assertTrue(self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0])

    def test_url_members_are_not_ingested(self):
        """A URL member is a reference. Nothing is copied for it, and it
        resolves to nothing locally -- the honest answer, not an error."""
        doc = self.series_doc(
            [self.write_member("a"), "https://nosuchserver.example/b.bin"],
            delete_original=0,
        )

        self.db.add_docs([doc], validate=False)

        self.assertTrue(self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0])
        self.assertFalse(
            self.db.exist_doc(doc.id(), "chunkdata.bin_2")[0],
            "nothing was copied in for a URL member",
        )

    def test_a_member_marked_not_to_be_ingested_is_left_where_it_is(self):
        location = self.write_member("a")
        doc = self.series_doc([location], ingest=0)

        self.db.add_docs([doc], validate=False)

        self.assertTrue(os.path.isfile(location), "not ingested, so not deleted")
        self.assertFalse(self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0])

    def test_a_missing_member_source_warns_and_the_document_still_lands(self):
        """Ingestion has always been non-fatal: the document is stored, its
        provenance is kept, and the member simply is not here."""
        location = self.write_member("a")
        doc = self.series_doc([location], delete_original=0)
        os.remove(location)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False)

        self.assertTrue(
            any("Failed to ingest series member" in str(w.message) for w in caught)
        )
        self.assertEqual(self.db.all_doc_ids(), [doc.id()])
        self.assertFalse(self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0])

    def test_an_already_ingested_member_is_not_copied_again(self):
        """Adding a document a second time -- to another branch -- must not
        re-copy members whose bytes are already in place. With the originals
        deleted there would be nothing to copy from."""
        doc = self.series_doc([self.write_member(n) for n in ("a", "b")])
        self.db.add_docs([doc], validate=False)
        after_first = self.file_dir_names()

        stored = self.db.get_docs(doc.id())
        self.db.add_branch("b")
        self.db.set_branch("b")
        with self.assertRaises(ValueError):
            # Reached the duplicate check, which is below the ingest loop and
            # below the guard: proof neither of them intervened.
            self.db.add_docs([stored], validate=False)

        self.assertEqual(self.file_dir_names(), after_first)

    def test_a_document_with_no_series_is_unaffected(self):
        location = self.write_member("filename1.ext")
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", location, delete_original=0)

        self.db.add_docs([doc], validate=False)

        self.assertTrue(self.db.exist_doc(doc.id(), "filename1.ext")[0])
        self.assertFalse(self.db.exist_doc(doc.id(), "filename1.ext_1")[0])


class TestRemoteSeriesMemberIngestion(SeriesDatabaseTestCase):
    """A member whose bytes are not a local path is retrieved by the caller's
    handler, exactly as a file_info location is. DID downloads nothing itself
    in either language."""

    def _handler(self, content=b"downloaded", produce=True):
        calls = []

        def handler(dest_path, source_path, context):
            calls.append((dest_path, source_path, dict(context)))
            if produce:
                with open(dest_path, "wb") as handle:
                    handle.write(content)

        return handler, calls

    def test_a_remote_member_is_ingested_through_the_handler(self):
        handler, calls = self._handler()
        doc = self.series_doc([REMOTE])
        uid = self.member_uids(doc)[0]

        self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertEqual(len(calls), 1)
        dest_path, source_path, _context = calls[0]
        self.assertEqual(source_path, REMOTE)
        self.assertEqual(dest_path, os.path.join(self.db._file_dir(), uid))
        self.assertTrue(self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0])

    def test_the_handler_is_told_which_series_member_it_is_fetching(self):
        """The DID-matlab#186 context. Without seriesName and uid a handler
        cannot tell a member from an ordinary file, and a member has no
        orig_location of its own to identify it."""
        handler, calls = self._handler()
        doc = self.series_doc([REMOTE, REMOTE], indices=[3, 4])
        uids = self.member_uids(doc)

        self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        contexts = [context for _dest, _src, context in calls]
        self.assertEqual(len(contexts), 2)
        for context, index, uid in zip(contexts, [3, 4], uids, strict=True):
            self.assertEqual(
                set(context),
                {"documentId", "filename", "seriesName", "uid", "mode"},
            )
            self.assertEqual(context["documentId"], doc.id())
            self.assertEqual(context["seriesName"], SERIES)
            self.assertEqual(context["filename"], f"{SERIES}_{index}")
            self.assertEqual(context["uid"], uid)
            # "add", matching MATLAB's do_add_doc. mode is a string the
            # caller's handler switches on, so a value that differed
            # between the languages would silently fall through a
            # handler's case. Python sent "ingest" until DID-python#71.
            self.assertEqual(context["mode"], "add")

    def test_a_two_argument_handler_is_still_called(self):
        """The older signature predates the context and must keep working."""
        calls = []

        def handler(dest_path, source_path):
            calls.append((dest_path, source_path))
            with open(dest_path, "wb") as handle:
                handle.write(b"downloaded")

        doc = self.series_doc([REMOTE])
        self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertEqual(len(calls), 1)
        self.assertTrue(self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0])

    def test_a_remote_member_without_a_handler_warns_but_still_adds(self):
        """Nothing can retrieve it, which warns rather than failing the add --
        the non-fatal behaviour ingestion has always had."""
        doc = self.series_doc([REMOTE])

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.db.add_docs([doc], validate=False)

        self.assertTrue(
            any("no custom_file_handler" in str(w.message) for w in caught),
            [str(w.message) for w in caught],
        )
        self.assertEqual(
            self.db.all_doc_ids(), [doc.id()], "the document is still added"
        )
        self.assertFalse(
            self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0],
            "nothing was retrieved, so nothing is there",
        )

    def test_a_handler_that_writes_nothing_leaves_the_document_addable(self):
        """A handler that returns cleanly having produced nothing must not
        fail the add, and must not leave a member that is not there.

        What this deliberately does NOT assert is whether anything is said
        about it. MATLAB checks isfile(destPath) after the handler returns
        and warns when it produced nothing; Python's series loop warns only
        if the handler raises, so this case passes in silence. That is
        DID-python#71 -- asserting the silence would lock the defect in, and
        asserting a warning would be a failing test for work this PR is not
        doing.
        """
        handler, calls = self._handler(produce=False)
        doc = self.series_doc([REMOTE])

        self.db.add_docs([doc], validate=False, custom_file_handler=handler)

        self.assertEqual(len(calls), 1, "the handler was given its chance")
        self.assertEqual(self.db.all_doc_ids(), [doc.id()])
        self.assertFalse(self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0])

    def test_a_remote_member_original_is_never_deleted(self):
        """delete_original only ever applies to a local path; there is no
        original to delete for something fetched over the network."""
        handler, _calls = self._handler()
        doc = self.series_doc([REMOTE], delete_original=1)

        # The point is that this does not raise trying to os.remove a URI.
        self.db.add_docs([doc], validate=False, custom_file_handler=handler)
        self.assertTrue(self.db.exist_doc(doc.id(), "chunkdata.bin_1")[0])


class TestUnsafeSeriesMemberUid(SeriesDatabaseTestCase):
    """A member uid becomes a filename under FileDir, so a value that escapes
    that directory once joined has to be refused. See DID-python#58."""

    def _doc_with_uid(self, uid):
        doc = self.series_doc([self.write_member("a")], delete_original=0)
        doc.series_ingest_locations(SERIES)[0]["uid"] = uid
        return doc

    def test_a_uid_that_is_not_a_plain_filename_is_refused(self):
        for uid in ("..", "../../escape", "a/b", "a\\b", " padded", "x\x00y"):
            with self.subTest(uid=uid):
                db = self.other_db(f"u{abs(hash(uid))}")
                doc = self._doc_with_uid(uid)
                with self.assertRaises(ValueError) as caught:
                    db.add_docs([doc], validate=False)
                self.assertIn("is not a plain filename", str(caught.exception))

    def test_the_refusal_leaves_nothing_stored(self):
        doc = self._doc_with_uid("../../escape")

        with self.assertRaises(ValueError):
            self.db.add_docs([doc], validate=False)

        self.assertEqual(self.db.all_doc_ids(), [])

    def test_nothing_is_written_outside_the_file_directory(self):
        doc = self._doc_with_uid("../escaped_member")

        with self.assertRaises(ValueError):
            self.db.add_docs([doc], validate=False)

        escaped = os.path.join(os.path.dirname(self.db._file_dir()), "escaped_member")
        self.assertFalse(os.path.exists(escaped))


class TestMembersNotLocatableGuard(SeriesDatabaseTestCase):
    """`_reject_series_without_ingest_locations`, DID-matlab#185.

    Both directions matter: it must fire on the round-tripped shape, and it
    must not fire on anything a caller legitimately authors.
    """

    def _stored_series_document(self):
        doc = self.series_doc([self.write_member(n) for n in ("a", "b")])
        self.db.add_docs([doc], validate=False)
        return self.db.get_docs(doc.id())

    def test_the_round_tripped_shape_is_what_the_guard_describes(self):
        """Guard the premise. If a stored document came back with its ingest
        locations intact, every test below would prove nothing."""
        stored = self._stored_series_document()

        self.assertEqual(stored.series_ingest_locations(SERIES), [])
        self.assertEqual(stored.series_count(SERIES), (2, 2))

    def test_a_stored_document_cannot_be_added_to_a_fresh_database(self):
        stored = self._stored_series_document()
        other = self.other_db()

        with self.assertRaises(ValueError) as caught:
            other.add_docs([stored], validate=False)
        message = str(caught.exception)
        self.assertIn("records no ingest_locations", message)
        self.assertIn(SERIES, message)
        self.assertIn(stored.id(), message)

    def test_the_refusal_happens_before_anything_is_written(self):
        """Refusing after the docs row went in would leave the broken
        document half-stored, which is the state the guard exists to
        prevent."""
        stored = self._stored_series_document()
        other = self.other_db()

        with self.assertRaises(ValueError):
            other.add_docs([stored], validate=False)

        self.assertEqual(other.all_doc_ids(), [])
        cursor = other.dbid.cursor()
        rows = cursor.execute(
            "SELECT doc_id FROM docs WHERE doc_id = ?", (stored.id(),)
        ).fetchall()
        self.assertEqual(rows, [])
        self.assertEqual(self.file_dir_names(other), [])

    def test_the_guard_exempts_a_document_the_database_already_holds(self):
        """The exemption that keeps the guard honest: re-adding a document
        this database already holds is ordinary, since its members were
        ingested when it first arrived.

        Reaching the duplicate check is the proof -- the guard sits above it
        and above every write, so it would have raised first.
        """
        stored = self._stored_series_document()

        with self.assertRaises(ValueError) as caught:
            self.db.add_docs([stored], validate=False)
        self.assertIn("already in branch", str(caught.exception))

    def test_a_declared_but_empty_series_is_not_refused(self):
        """A series that was never populated has nothing to locate."""
        doc = Document("demoSeries", **{"demoSeries.value": 1})

        self.db.add_docs([doc], validate=False)

        self.assertEqual(self.db.all_doc_ids(), [doc.id()])

    def test_a_freshly_authored_series_is_not_refused(self):
        """The false-positive control. The guard raises out of add_docs, so
        one that fired here would brick every ordinary caller."""
        doc = self.series_doc([self.write_member("a")], delete_original=0)

        self.db.add_docs([doc], validate=False)

        self.assertEqual(self.db.all_doc_ids(), [doc.id()])

    def test_a_sparse_series_is_judged_on_what_is_present_not_on_slots(self):
        """n_present, not count: a one-member series in a hundred slots has
        one location to find, and ninety-nine slots that need none."""
        doc = self.series_doc(
            [self.write_member("a")], indices=[100], delete_original=0
        )

        self.db.add_docs([doc], validate=False)

        self.assertEqual(doc.series_count(SERIES), (100, 1))
        self.assertEqual(self.db.all_doc_ids(), [doc.id()])

    def test_a_document_with_no_series_at_all_is_not_refused(self):
        doc = Document("demoFile", **{"demoFile.value": 1})
        doc.add_file("filename1.ext", self.write_member("f"), delete_original=0)

        self.db.add_docs([doc], validate=False)

        self.assertEqual(self.db.all_doc_ids(), [doc.id()])


if __name__ == "__main__":
    unittest.main()
