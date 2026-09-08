"""open_doc resolving a file series member.

Ports DID-matlab's ``TestSeriesMemberFetch.m`` and the read-side half of
``TestFileSeriesRoundTrip.m``. See VH-Lab/DID-matlab#173, #188 and #191.

A series member is the one file DID can be asked for that has no
``orig_location`` of its own -- that is a per-member record a series
deliberately does not keep, since keeping 28,000 of them is the cost the
design exists to avoid. So ``_open_series_member`` resolves it in four
steps, and each step has its own way of failing:

1. find the series MANIFEST on this machine        -> ManifestNotLocal
2. read the member's uid out of the manifest slot  -> ManifestUnreadable
                                                   -> NoSuchMember
3. look for those bytes in the local caches
4. failing that, offer the MANIFEST's locations to custom_file_handler
   with the member's uid in the context (DID-matlab#188), and refuse a
   handler that answers with the manifest's own bytes (DID-matlab#191)
                                                   -> MemberNotHere
                                                   -> MemberFetchFailed
                                                   -> HandlerReturnedManifest

Every one of those six identifiers is asserted below; none was asserted
anywhere before. DID composes no URL and learns no scheme -- what the
handler is offered is a location the document already carried.

The cloud shape these tests use -- a document whose manifest lives remotely
and whose members are not on this machine -- is built by storing a document
whose manifest location is remote and then removing the ingested members. It
is the shape NDI-python#215 is about.
"""

import glob
import os
import shutil
import tempfile
import unittest
from unittest import mock

from did.common import PathConstants, get_cache
from did.database import FileAccessError, _series_manifest_path
from did.document import Document
from did.file import ReadOnlyFileobj, cached_path_for_uid
from did.implementations.sqlitedb import SQLiteDB

SERIES = "chunkdata.bin"
REMOTE_MANIFEST = "ndic://d-123/manifest"


class SeriesReadTestCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()
        self.db = SQLiteDB(os.path.join(self._dir, "t.sqlite"))
        self.db.add_branch("a")
        self.db.set_branch("a")

    def tearDown(self):
        self.db._close_db()

    def _write_members(self, count, indices=None):
        store = os.path.join(self._dir, "store")
        os.makedirs(store, exist_ok=True)
        locations = []
        for i in range(count):
            path = os.path.join(store, f"m{i}")
            with open(path, "wb") as handle:
                handle.write(f"member-{i}-bytes".encode())
            locations.append(path)
        return locations

    def stored_series(self, count=2, indices=None, remote_manifest=False):
        """Store a demoSeries document and hand back the document as read.

        With ``remote_manifest`` the manifest's only recorded location is a
        cloud URI, which is the shape a document has once its files live in
        a store rather than on this disk. The manifest's BYTES are still
        ingested into FileDir, because a series cannot be resolved at all
        without a readable manifest.
        """
        locations = self._write_members(count)
        doc = Document("demoSeries", **{"demoSeries.value": 1})
        doc.add_file_series(SERIES, locations, indices=indices, delete_original=0)

        _is_in, info, _ = doc.is_in_file_list(SERIES)
        with open(info["locations"][0]["location"], "rb") as handle:
            self.manifest_bytes = handle.read()
        manifest_uid = info["locations"][0]["uid"]

        handler = None
        if remote_manifest:
            info["locations"] = [
                {
                    "location": REMOTE_MANIFEST,
                    "location_type": "ndicloud",
                    "uid": manifest_uid,
                    "ingest": 1,
                    "delete_original": 0,
                    "parameters": "",
                }
            ]
            manifest_bytes = self.manifest_bytes

            def handler(dest_path, source_path):
                with open(dest_path, "wb") as handle:
                    handle.write(manifest_bytes)

        self.member_uids = [e["uid"] for e in doc.series_ingest_locations(SERIES)]
        self.manifest_uid = manifest_uid
        self.db.add_docs([doc], validate=False, custom_file_handler=handler)
        return self.db.get_docs(doc.id())

    def evict_members(self):
        """Remove the ingested member copies: the "not on this machine" state."""
        for uid in self.member_uids:
            path = os.path.join(self.db._file_dir(), uid)
            if os.path.exists(path):
                os.remove(path)

    def manifest_path(self, doc):
        return _series_manifest_path(doc, SERIES, self.db._do_cached_path_roots())

    def read(self, file_obj):
        file_obj.fopen()
        try:
            return file_obj.fread()
        finally:
            file_obj.fclose()

    def recording_handler(self, content=b"fetched-from-the-store"):
        calls = []

        def handler(dest_path, source_path, context):
            calls.append((dest_path, source_path, dict(context)))
            with open(dest_path, "wb") as handle:
                handle.write(content)

        return handler, calls


class TestOpeningALocalMember(SeriesReadTestCase):
    def test_a_member_that_is_here_is_opened_with_no_handler(self):
        doc = self.stored_series(count=3)

        file_obj = self.db.open_doc(doc.id(), "chunkdata.bin_2")

        self.assertIsInstance(file_obj, ReadOnlyFileobj)
        self.assertEqual(self.read(file_obj), b"member-1-bytes")

    def test_each_member_opens_its_own_bytes(self):
        doc = self.stored_series(count=3)

        for index in (1, 2, 3):
            with self.subTest(index=index):
                file_obj = self.db.open_doc(doc.id(), f"{SERIES}_{index}")
                self.assertEqual(
                    self.read(file_obj), f"member-{index - 1}-bytes".encode()
                )

    def test_a_member_that_is_here_never_reaches_the_handler(self):
        doc = self.stored_series(count=2)
        handler, calls = self.recording_handler()

        self.db.open_doc(doc.id(), "chunkdata.bin_1", custom_file_handler=handler)

        self.assertEqual(calls, [], "a local hit must not fetch")

    def test_exist_doc_and_open_doc_agree_about_a_member(self):
        doc = self.stored_series(count=2)

        exists, path = self.db.exist_doc(doc.id(), "chunkdata.bin_1")
        self.assertTrue(exists)
        self.assertEqual(path, os.path.join(self.db._file_dir(), self.member_uids[0]))
        self.assertEqual(
            self.read(self.db.open_doc(doc.id(), "chunkdata.bin_1")), b"member-0-bytes"
        )

    def test_a_sparse_series_opens_the_slots_it_fills(self):
        doc = self.stored_series(count=2, indices=[1, 5])

        self.assertEqual(
            self.read(self.db.open_doc(doc.id(), "chunkdata.bin_5")), b"member-1-bytes"
        )

    def test_a_name_that_is_not_a_member_is_not_treated_as_one(self):
        """An undeclared NAME_# resolves through neither mechanism, and must
        say so as an ordinary missing file rather than as a series error."""
        doc = self.stored_series(count=2)

        with self.assertRaises(FileNotFoundError) as caught:
            self.db.open_doc(doc.id(), "nosuch.bin_5")
        self.assertNotIsInstance(caught.exception, FileAccessError)


class TestSeriesMemberFailures(SeriesReadTestCase):
    """The six FileAccessError identifiers, none of which was asserted
    anywhere before. They are the only thing a caller can branch on, so a
    wrong one is as bad as no error."""

    def test_a_manifest_that_is_not_here_is_manifest_not_local(self):
        doc = self.stored_series(count=2)
        os.remove(os.path.join(self.db._file_dir(), self.manifest_uid))

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "chunkdata.bin_1")
        self.assertEqual(
            caught.exception.identifier, "DID:SQLITEDB:FileSeries:ManifestNotLocal"
        )
        self.assertIn(SERIES, str(caught.exception))
        self.assertIn(doc.id(), str(caught.exception))

    def test_a_manifest_that_cannot_be_parsed_is_manifest_unreadable(self):
        """Reported as its own failure rather than read as an absent member:
        a corrupt manifest is a different problem from an empty slot, and
        silently answering "no such member" would hide it."""
        doc = self.stored_series(count=2)
        with open(os.path.join(self.db._file_dir(), self.manifest_uid), "wb") as handle:
            handle.write(b"this is not a series manifest")

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "chunkdata.bin_1")
        self.assertEqual(
            caught.exception.identifier, "DID:SQLITEDB:FileSeries:ManifestUnreadable"
        )

    def test_a_slot_beyond_the_series_is_no_such_member(self):
        doc = self.stored_series(count=2)

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "chunkdata.bin_9")
        self.assertEqual(
            caught.exception.identifier, "DID:SQLITEDB:FileSeries:NoSuchMember"
        )

    def test_an_empty_slot_of_a_sparse_series_is_no_such_member(self):
        doc = self.stored_series(count=2, indices=[1, 5])

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "chunkdata.bin_3")
        self.assertEqual(
            caught.exception.identifier, "DID:SQLITEDB:FileSeries:NoSuchMember"
        )

    def test_an_absent_member_with_no_handler_is_member_not_here(self):
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "chunkdata.bin_1")
        self.assertEqual(
            caught.exception.identifier, "DID:SQLITEDB:FileSeries:MemberNotHere"
        )
        self.assertIn("no custom_file_handler", str(caught.exception))

    def test_a_handler_with_nowhere_to_look_is_member_not_here(self):
        """The manifest's only recorded location is the temporary file
        add_file_series wrote, which ingestion consumed. There is nothing to
        offer, and offering nothing is not the same as a failed fetch."""
        doc = self.stored_series(count=2)
        self.evict_members()
        handler, calls = self.recording_handler()

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "chunkdata.bin_1", custom_file_handler=handler)
        self.assertEqual(
            caught.exception.identifier, "DID:SQLITEDB:FileSeries:MemberNotHere"
        )
        self.assertIn("no location a handler could reach", str(caught.exception))
        self.assertEqual(calls, [], "nothing was offered, so nothing was called")

    def test_a_handler_that_raises_is_member_fetch_failed(self):
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()

        def handler(dest_path, source_path, context):
            raise RuntimeError("the store said no")

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "chunkdata.bin_1", custom_file_handler=handler)
        self.assertEqual(
            caught.exception.identifier, "DID:SQLITEDB:FileSeries:MemberFetchFailed"
        )
        self.assertIn("the store said no", str(caught.exception))

    def test_a_handler_that_writes_nothing_is_member_fetch_failed(self):
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()

        def handler(dest_path, source_path, context):
            return None

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "chunkdata.bin_1", custom_file_handler=handler)
        self.assertEqual(
            caught.exception.identifier, "DID:SQLITEDB:FileSeries:MemberFetchFailed"
        )

    def test_every_identifier_is_a_file_not_found_error(self):
        """FileAccessError subclasses FileNotFoundError so a caller that only
        knows the built-in still handles a missing member."""
        doc = self.stored_series(count=2)

        with self.assertRaises(FileNotFoundError):
            self.db.open_doc(doc.id(), "chunkdata.bin_9")


class TestFetchingAnAbsentMember(SeriesReadTestCase):
    """DID-matlab#188. A member has no location of its own, so the handler is
    offered the MANIFEST's location plus the member's uid, and decides how to
    turn the two into a fetch. DID composes no URL and learns no scheme."""

    def test_an_absent_member_is_fetched_through_the_handler(self):
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        handler, calls = self.recording_handler()

        file_obj = self.db.open_doc(
            doc.id(), "chunkdata.bin_1", custom_file_handler=handler
        )

        self.assertEqual(self.read(file_obj), b"fetched-from-the-store")
        self.assertEqual(len(calls), 1)

    def test_the_handler_is_offered_the_manifests_own_location(self):
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        handler, calls = self.recording_handler()

        self.db.open_doc(doc.id(), "chunkdata.bin_1", custom_file_handler=handler)

        _dest, source_path, _context = calls[0]
        self.assertEqual(
            source_path,
            REMOTE_MANIFEST,
            "the location the document already carried, not one DID composed",
        )

    def test_the_context_names_the_series_and_the_members_own_uid(self):
        """The uid is what distinguishes a member from its manifest: both
        arrive with the same sourcePath, and only the context says which
        object in the store is wanted."""
        doc = self.stored_series(count=3, remote_manifest=True)
        self.evict_members()
        handler, calls = self.recording_handler()

        self.db.open_doc(doc.id(), "chunkdata.bin_2", custom_file_handler=handler)

        context = calls[0][2]
        self.assertEqual(
            set(context), {"documentId", "filename", "seriesName", "uid", "mode"}
        )
        self.assertEqual(context["documentId"], doc.id())
        self.assertEqual(context["filename"], "chunkdata.bin_2")
        self.assertEqual(context["seriesName"], SERIES)
        self.assertEqual(context["uid"], self.member_uids[1])
        self.assertNotEqual(
            context["uid"], self.manifest_uid, "the member's uid, not the manifest's"
        )
        self.assertEqual(context["mode"], "open")

    def test_each_member_is_fetched_under_its_own_uid(self):
        doc = self.stored_series(count=3, remote_manifest=True)
        self.evict_members()
        handler, calls = self.recording_handler()

        for index in (1, 2, 3):
            self.db.open_doc(doc.id(), f"{SERIES}_{index}", custom_file_handler=handler)

        fetched = [context["uid"] for _dest, _src, context in calls]
        self.assertEqual(fetched, self.member_uids)

    def test_a_two_argument_handler_is_still_called(self):
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        calls = []

        def handler(dest_path, source_path):
            calls.append(source_path)
            with open(dest_path, "wb") as handle:
                handle.write(b"two-arg")

        file_obj = self.db.open_doc(
            doc.id(), "chunkdata.bin_1", custom_file_handler=handler
        )

        self.assertEqual(self.read(file_obj), b"two-arg")
        self.assertEqual(calls, [REMOTE_MANIFEST])

    def test_a_fetched_member_is_kept_and_not_fetched_again(self):
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        handler, calls = self.recording_handler()

        first = self.db.open_doc(
            doc.id(), "chunkdata.bin_1", custom_file_handler=handler
        )
        self.assertEqual(self.read(first), b"fetched-from-the-store")

        second = self.db.open_doc(
            doc.id(), "chunkdata.bin_1", custom_file_handler=handler
        )

        self.assertEqual(self.read(second), b"fetched-from-the-store")
        self.assertEqual(len(calls), 1, "the second open came from the cache")

    def test_a_failed_fetch_leaves_no_partial_behind(self):
        """A half-written download that survived would be indistinguishable
        from a complete one on the next open."""
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        temp_dir = PathConstants().temppath
        os.makedirs(temp_dir, exist_ok=True)
        before = set(glob.glob(os.path.join(temp_dir, "*")))

        def handler(dest_path, source_path, context):
            with open(dest_path, "wb") as handle:
                handle.write(b"half")
            raise RuntimeError("connection dropped")

        with self.assertRaises(FileAccessError):
            self.db.open_doc(doc.id(), "chunkdata.bin_1", custom_file_handler=handler)

        self.assertEqual(set(glob.glob(os.path.join(temp_dir, "*"))) - before, set())

    def test_exist_doc_never_fetches(self):
        """exist_doc reports on local state. Answering it by downloading
        would make a "is this here?" loop pull a whole pyramid level."""
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()

        exists, path = self.db.exist_doc(doc.id(), "chunkdata.bin_1")

        self.assertFalse(exists, "not here is the honest answer, not an error")
        self.assertIsNone(path)
        self.assertFalse(
            os.path.exists(os.path.join(self.db._file_dir(), self.member_uids[0])),
            "and asking did not bring it here",
        )


class TestHandlerReturnedManifestGuard(SeriesReadTestCase):
    """DID-matlab#191. The handler is given the manifest's location, so a
    handler that resolves the location instead of the uid answers with the
    manifest's own bytes -- which would be cached under the member's uid and
    silently corrupt every later read of that member."""

    def test_a_handler_that_returns_the_manifests_bytes_is_refused(self):
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        manifest_path = self.manifest_path(doc)

        def handler(dest_path, source_path, context):
            shutil.copyfile(manifest_path, dest_path)

        with self.assertRaises(FileAccessError) as caught:
            self.db.open_doc(doc.id(), "chunkdata.bin_1", custom_file_handler=handler)
        self.assertEqual(
            caught.exception.identifier,
            "DID:SQLITEDB:FileSeries:HandlerReturnedManifest",
        )
        self.assertIn("DID-matlab #191", str(caught.exception))

    def test_a_correct_fetch_still_works_after_the_guard_has_fired(self):
        """The regression test for DID-python#73, and the property that
        matters most: refusing must cost nothing beyond this one attempt.

        `_fetch_remote_to_cache` MOVES its download into the file cache and
        returns the path inside it, so the guard's removal has to go through
        the cache. A bare os.remove took the file and left the index entry,
        and `cache.is_file(uid)` then answered True forever for a file that
        was not there: every later fetch short circuited on the index and
        handed back the missing path, which Fileobj.fopen() reads as b""
        without raising. The guard against silent corruption was itself
        silently corrupting the member, permanently.
        """
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        manifest_path = self.manifest_path(doc)

        def returns_manifest(dest_path, source_path, context):
            shutil.copyfile(manifest_path, dest_path)

        with self.assertRaises(FileAccessError):
            self.db.open_doc(
                doc.id(), "chunkdata.bin_1", custom_file_handler=returns_manifest
            )

        calls = []

        def returns_member(dest_path, source_path, context):
            calls.append(source_path)
            with open(dest_path, "wb") as handle:
                handle.write(b"the real member")

        file_obj = self.db.open_doc(
            doc.id(), "chunkdata.bin_1", custom_file_handler=returns_member
        )

        self.assertEqual(
            len(calls), 1, "the retry must reach the handler, not a stale index entry"
        )
        self.assertEqual(self.read(file_obj), b"the real member")

    def test_the_refused_bytes_are_not_left_in_the_cache(self):
        """Stated directly, since it is the mechanism rather than the
        symptom: after the refusal the cache must not claim to hold the
        member at all."""
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        manifest_path = self.manifest_path(doc)
        member_uid = self.member_uids[0]

        def returns_manifest(dest_path, source_path, context):
            shutil.copyfile(manifest_path, dest_path)

        with self.assertRaises(FileAccessError):
            self.db.open_doc(
                doc.id(), "chunkdata.bin_1", custom_file_handler=returns_manifest
            )

        cache = get_cache()
        self.assertFalse(
            cache.is_file(member_uid),
            "the index must go with the file, not outlive it",
        )
        self.assertIsNone(
            cached_path_for_uid(
                member_uid, additional_roots=self.db._do_cached_path_roots()
            )
        )

    def test_a_fetch_that_cannot_be_measured_is_a_failure_not_a_pass(self):
        """The third arm of DID-python#73, and the one that turned the stale
        cache entry into an empty read rather than an error.

        The guard used to read a getsize failure as "comparison unavailable;
        accept the fetch". But a file that was just fetched and cannot even
        be measured is not a file to hand back -- and Fileobj.fopen()
        swallows the OSError, so the caller reads b"" and sees nothing wrong.
        Forced here by handing back a path that does not exist, which is
        exactly what the stale index entry used to produce.
        """
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        missing = os.path.join(self._dir, "never-written")
        self.assertFalse(os.path.exists(missing))

        with (
            mock.patch.object(
                SQLiteDB, "_fetch_remote_to_cache", return_value=(missing, None)
            ),
            self.assertRaises(FileAccessError) as caught,
        ):
            self.db.open_doc(
                doc.id(),
                "chunkdata.bin_1",
                custom_file_handler=self.recording_handler()[0],
            )

        self.assertEqual(
            caught.exception.identifier, "DID:SQLITEDB:FileSeries:MemberFetchFailed"
        )
        self.assertIn("unreadable", str(caught.exception))

    def test_a_cache_entry_whose_file_has_gone_is_refetched(self):
        """The same defect from the other side, and the reason the fix is in
        two places. A cache index entry can go stale without the guard --
        someone clears the cache directory by hand, an eviction races a
        read -- and the fetch path's post-lock re-check trusted the index
        alone. It now checks the filesystem, so a stale entry costs one
        refetch rather than an empty read forever."""
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        handler, calls = self.recording_handler(content=b"first fetch")

        first = self.db.open_doc(
            doc.id(), "chunkdata.bin_1", custom_file_handler=handler
        )
        self.assertEqual(self.read(first), b"first fetch")
        self.assertEqual(len(calls), 1)

        # Go behind the cache's back, as a hand-cleared cache directory does.
        cache = get_cache()
        member_uid = self.member_uids[0]
        self.assertTrue(cache.is_file(member_uid))
        os.remove(cache.full_path(member_uid))

        second_handler, second_calls = self.recording_handler(content=b"second fetch")
        second = self.db.open_doc(
            doc.id(), "chunkdata.bin_1", custom_file_handler=second_handler
        )

        self.assertEqual(len(second_calls), 1, "the stale entry must not be trusted")
        self.assertEqual(self.read(second), b"second fetch")

    def test_the_manifest_itself_is_still_readable_as_an_ordinary_file(self):
        """The guard must not make the manifest unopenable: it is a file of
        the document like any other, and NAME is a valid name to ask for."""
        doc = self.stored_series(count=2, remote_manifest=True)

        file_obj = self.db.open_doc(doc.id(), SERIES)

        self.assertEqual(self.read(file_obj), self.manifest_bytes)

    def test_a_member_the_same_size_as_the_manifest_is_still_accepted(self):
        """The guard compares BYTES, with size only as a prefilter. A member
        that happens to be the manifest's length is not the manifest, and
        refusing it would be a false positive on ordinary data."""
        doc = self.stored_series(count=2, remote_manifest=True)
        self.evict_members()
        same_length_different_bytes = b"\x00" * len(self.manifest_bytes)
        self.assertNotEqual(same_length_different_bytes, self.manifest_bytes)

        def handler(dest_path, source_path, context):
            with open(dest_path, "wb") as handle:
                handle.write(same_length_different_bytes)

        file_obj = self.db.open_doc(
            doc.id(), "chunkdata.bin_1", custom_file_handler=handler
        )

        self.assertEqual(self.read(file_obj), same_length_different_bytes)


class TestManifestLocationsForHandler(SeriesReadTestCase):
    """Which of the manifest's locations get offered, and in what order."""

    def _doc_with_manifest_locations(self, locations):
        doc = self.stored_series(count=2)
        _is_in, info, _ = doc.is_in_file_list(SERIES)
        info["locations"] = locations
        return doc

    def test_remote_locations_come_before_local_ones(self):
        """A handler that can answer from the manifest's store should not be
        handed a local copy it could only re-download from itself. Note that
        the local one is still offered: the #191 guard makes an unhelpful
        answer harmless rather than incorrect."""
        doc = self.stored_series(count=2)
        local = os.path.join(self._dir, "manifest-copy")
        shutil.copyfile(self.manifest_path(doc), local)
        _is_in, info, _ = doc.is_in_file_list(SERIES)
        info["locations"] = [
            {"location": local, "location_type": "file"},
            {"location": REMOTE_MANIFEST, "location_type": "ndicloud"},
        ]

        offered = self.db._manifest_locations_for_handler(doc, SERIES)

        self.assertEqual(offered, [REMOTE_MANIFEST, local])

    def test_a_local_location_that_is_not_there_is_not_offered(self):
        """A stale entry would just be a failing round trip."""
        doc = self._doc_with_manifest_locations(
            [
                {"location": os.path.join(self._dir, "gone"), "location_type": "file"},
                {"location": REMOTE_MANIFEST, "location_type": "ndicloud"},
            ]
        )

        self.assertEqual(
            self.db._manifest_locations_for_handler(doc, SERIES), [REMOTE_MANIFEST]
        )

    def test_a_location_with_no_path_is_skipped(self):
        doc = self._doc_with_manifest_locations(
            [{"location": "", "location_type": "file"}, {"location": REMOTE_MANIFEST}]
        )

        self.assertEqual(
            self.db._manifest_locations_for_handler(doc, SERIES), [REMOTE_MANIFEST]
        )

    def test_a_single_dict_of_locations_is_accepted(self):
        """The MATLAB-written shape: one location is a struct, not an array."""
        doc = self._doc_with_manifest_locations(
            {"location": REMOTE_MANIFEST, "location_type": "ndicloud"}
        )

        self.assertEqual(
            self.db._manifest_locations_for_handler(doc, SERIES), [REMOTE_MANIFEST]
        )

    def test_a_name_the_document_has_no_manifest_for_offers_nothing(self):
        doc = self.stored_series(count=2)

        self.assertEqual(self.db._manifest_locations_for_handler(doc, "nosuch.bin"), [])


if __name__ == "__main__":
    unittest.main()
