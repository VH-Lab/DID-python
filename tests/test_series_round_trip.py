"""A file series through storage and back, and the accessors that walk one.

Ports the round-trip half of DID-matlab's ``TestFileSeriesRoundTrip.m``. See
VH-Lab/DID-matlab#173. The document API, the add path and ``open_doc`` have
their own files; this is the whole loop -- author, store, read back, resolve
-- plus ``did.database``'s ``series_has`` and ``series_members``.

Two claims are being tested, and they are the ones the feature rests on:

* **A stored document still resolves its members.** ``add_docs`` strips the
  member paths on the way in, so everything a reader needs afterwards is the
  DECLARATION plus the manifest. If that were not true, stripping would be
  data loss rather than tidying.
* **Resolution reaches no database.** ``cached_path_for_file``,
  ``series_has`` and ``series_members`` answer from the document in hand and
  the filesystem. A viewer walking a 28,000-member pyramid level cannot hold
  a database session per worker, so a query hidden in any of the three would
  undo the point of the feature. The ``NoQueryDatabase`` below raises on
  every ``do_*`` method, so a passing test means none was reached -- mirroring
  MATLAB's ``did.test.helper.NoQueryDatabaseWithRoots``.
"""

import json
import os
import tempfile
import unittest
import warnings

from did.database import Database
from did.document import Document
from did.file import cached_path_for_uid, read_series_manifest
from did.implementations.sqlitedb import SQLiteDB

SERIES = "chunkdata.bin"
MANIFEST_MAGIC = b"DIDFSER1"


class NoQueryDatabase(Database):
    """A database that raises on every operation, with uid-named roots.

    Mirrors MATLAB's ``did.test.helper.NoQueryDatabaseWithRoots``. Only
    ``_do_cached_path_roots`` answers; anything that would touch storage is a
    failure, so a test that passes with one of these proves the code path
    under test asked nothing of the database.
    """

    def __init__(self, roots):
        super().__init__(connection="")
        self._roots = list(roots)

    def _do_cached_path_roots(self):
        return self._roots

    def _refuse(self, *args, **kwargs):
        raise AssertionError(
            "This resolution must not reach the database: it is what a "
            "caller walking a pyramid level relies on."
        )

    _open_db = _close_db = _refuse
    _do_get_branch_ids = _do_add_branch = _refuse
    _do_get_doc_ids = _do_add_doc = _do_get_doc = _do_remove_doc = _refuse
    _do_delete_branch = _do_get_sub_branches = _do_get_branch_parent = _refuse
    do_run_sql_query = _refuse

    def __del__(self):
        pass  # the base class's destructor would call the refusing _close_db


class SeriesRoundTripTestCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()
        self.db = SQLiteDB(os.path.join(self._dir, "t.sqlite"))
        self.db.add_branch("a")
        self.db.set_branch("a")

    def tearDown(self):
        self.db._close_db()

    def ingested_series(self, indices=None, count=3):
        """Author a series, store it, and hand back everything to check.

        Returns ``(doc, locations, contents)``: the document as authored (the
        stored one is a ``get_docs`` away), the member source paths, and each
        member's bytes in the order given.
        """
        store = os.path.join(self._dir, "store")
        os.makedirs(store, exist_ok=True)
        locations = []
        contents = []
        for i in range(count):
            payload = bytes([i + 1]) * (10 + i)
            path = os.path.join(store, f"m{i}.bin")
            with open(path, "wb") as handle:
                handle.write(payload)
            locations.append(path)
            contents.append(payload)

        doc = Document("demoSeries", **{"demoSeries.value": 1})
        doc.add_file_series(SERIES, locations, indices=indices, delete_original=0)
        self.member_uids = [e["uid"] for e in doc.series_ingest_locations(SERIES)]
        self.db.add_docs([doc], validate=False)
        return doc, locations, contents

    def read(self, doc_id, filename):
        file_obj = self.db.open_doc(doc_id, filename)
        file_obj.fopen()
        try:
            return file_obj.fread()
        finally:
            file_obj.fclose()

    def manifest_of(self, doc):
        exists, path = self.db.exist_doc(doc.id(), SERIES)
        self.assertTrue(exists, "precondition: the manifest itself is ingested")
        return read_series_manifest(path)


class TestMembersSurviveTheRoundTrip(SeriesRoundTripTestCase):
    def test_every_member_reads_back_byte_for_byte(self):
        """The test the whole feature exists for."""
        doc, _locations, contents = self.ingested_series()

        for index, expected in enumerate(contents, start=1):
            name = f"{SERIES}_{index}"
            with self.subTest(member=name):
                self.assertTrue(self.db.exist_doc(doc.id(), name)[0])
                self.assertEqual(self.read(doc.id(), name), expected)

    def test_each_member_resolves_to_its_own_bytes(self):
        """A resolution that is off by one, or that returns the manifest, or
        that returns whichever member is first, would pass a test that only
        checked that something came back."""
        doc, _locations, contents = self.ingested_series()

        first = self.read(doc.id(), "chunkdata.bin_1")
        second = self.read(doc.id(), "chunkdata.bin_2")
        third = self.read(doc.id(), "chunkdata.bin_3")

        self.assertEqual([first, second, third], contents)
        self.assertNotEqual(first, second)

    def test_members_land_under_their_manifest_uid(self):
        """The pairing the add side and the read side share. Under any other
        name the manifest would point at nothing."""
        doc, _locations, _contents = self.ingested_series()
        manifest = self.manifest_of(doc)

        for slot, uid in enumerate(manifest["uids"], start=1):
            with self.subTest(slot=slot):
                self.assertTrue(uid)
                self.assertTrue(os.path.isfile(os.path.join(self.db._file_dir(), uid)))

    def test_the_manifest_is_still_readable_as_an_ordinary_file(self):
        """A series' declared name is its manifest, an ordinary file in
        file_list. Nothing about member resolution may change that: code
        that knows nothing about series still carries it."""
        doc, _locations, _contents = self.ingested_series()

        self.assertEqual(self.read(doc.id(), SERIES)[:8], MANIFEST_MAGIC)


class TestTheStoredDocument(SeriesRoundTripTestCase):
    def test_it_carries_no_member_paths(self):
        """Asserted after a real add_docs, not on
        strip_series_ingest_locations in isolation."""
        doc, locations, _contents = self.ingested_series()

        stored = self.db.get_docs(doc.id())
        encoded = json.dumps(stored.document_properties)
        for location in locations:
            self.assertNotIn(location, encoded)

    def test_it_still_knows_the_series_and_its_size(self):
        doc, _locations, _contents = self.ingested_series()

        stored = self.db.get_docs(doc.id())
        self.assertEqual(stored.series_names(), [SERIES])
        self.assertEqual(stored.series_count(SERIES), (3, 3))
        self.assertEqual(
            stored.series_ingest_locations(SERIES),
            [],
            "precondition: no member paths are left",
        )

    def test_it_still_resolves_a_member(self):
        """Everything needed afterwards is the declaration plus the manifest,
        which is exactly the claim stripping rests on."""
        doc, _locations, contents = self.ingested_series()
        stored = self.db.get_docs(doc.id())

        exists, path = self.db.cached_path_for_file(stored, "chunkdata.bin_2")

        self.assertTrue(exists)
        with open(path, "rb") as handle:
            self.assertEqual(handle.read(), contents[1])

    def test_exist_doc_and_cached_path_for_file_agree(self):
        """The two resolutions of a member -- one through the database, one
        from the document in hand -- must give the same file. They differ
        only in what they need to get there."""
        doc, _locations, _contents = self.ingested_series()

        for index in (1, 2, 3):
            name = f"{SERIES}_{index}"
            with self.subTest(member=name):
                exists, by_id = self.db.exist_doc(doc.id(), name)
                cached, by_doc = self.db.cached_path_for_file(doc, name)
                self.assertTrue(exists)
                self.assertEqual(cached, exists)
                self.assertEqual(os.path.abspath(by_doc), by_id)


class TestCachedPathForFile(SeriesRoundTripTestCase):
    """The no-SQL, no-network lookup both mechanisms share. It arrived with
    the series work and had no test of its own; its file_info branch is what
    a series member's miss falls through from."""

    def test_an_ordinary_file_resolves_through_its_own_uid(self):
        """The manifest is an ordinary file, so it takes the file_info branch
        rather than the series one."""
        doc, _locations, _contents = self.ingested_series()

        exists, path = self.db.cached_path_for_file(doc, SERIES)

        self.assertTrue(exists)
        self.assertEqual(
            path, os.path.join(self.db._file_dir(), doc.file_uids(SERIES)[0])
        )

    def test_a_recorded_file_whose_bytes_are_gone_is_not_here(self):
        """ "Not on this machine yet" -- not "no such file"."""
        doc, _locations, _contents = self.ingested_series()
        os.remove(os.path.join(self.db._file_dir(), doc.file_uids(SERIES)[0]))

        self.assertEqual(self.db.cached_path_for_file(doc, SERIES), (False, None))

    def test_a_file_the_document_does_not_have_is_not_here(self):
        doc, _locations, _contents = self.ingested_series()

        self.assertEqual(
            self.db.cached_path_for_file(doc, "plainfile.ext"), (False, None)
        )
        self.assertEqual(self.db.cached_path_for_file(doc, "nosuch.ext"), (False, None))

    def test_it_answers_about_the_document_in_hand(self):
        """Not about the document in the database: the uid comes from the
        object, which is what makes the lookup query-free."""
        doc, _locations, contents = self.ingested_series()
        stored = self.db.get_docs(doc.id())

        self.assertEqual(
            self.db.cached_path_for_file(stored, "chunkdata.bin_1"),
            self.db.cached_path_for_file(doc, "chunkdata.bin_1"),
        )
        with open(self.db.cached_path_for_file(doc, "chunkdata.bin_1")[1], "rb") as h:
            self.assertEqual(h.read(), contents[0])


class TestSparseSeriesResolution(SeriesRoundTripTestCase):
    def test_only_the_present_slots_resolve(self):
        """The case a NAME_# entry cannot express: the gaps must answer no,
        and the present ones must answer with their own bytes rather than a
        neighbour's."""
        doc, _locations, contents = self.ingested_series(indices=[2, 5, 6])

        for slot, expected in zip([2, 5, 6], contents, strict=True):
            name = f"{SERIES}_{slot}"
            with self.subTest(member=name):
                self.assertTrue(self.db.exist_doc(doc.id(), name)[0])
                self.assertEqual(self.read(doc.id(), name), expected)

        for slot in (1, 3, 4):
            with self.subTest(absent=slot):
                self.assertFalse(
                    self.db.exist_doc(doc.id(), f"{SERIES}_{slot}")[0],
                    "an absent slot must not resolve to a neighbour",
                )

    def test_a_member_beyond_the_series_does_not_exist(self):
        doc, _locations, _contents = self.ingested_series()

        self.assertFalse(self.db.exist_doc(doc.id(), "chunkdata.bin_99")[0])

    def test_an_undeclared_numbered_name_is_still_rejected(self):
        """The series rule must not turn every NAME_<n> into a resolvable
        file. plainfile.ext is declared as an ordinary file, not a series."""
        doc, _locations, _contents = self.ingested_series()

        self.assertFalse(self.db.exist_doc(doc.id(), "plainfile.ext_1")[0])
        with self.assertRaises(FileNotFoundError):
            self.db.open_doc(doc.id(), "plainfile.ext_1")


class TestSeriesAccessors(SeriesRoundTripTestCase):
    """series_count answers from the document; WHICH slots are filled is
    recorded only in the manifest, so series_has and series_members read it.
    Both still run no query and touch no network."""

    def test_series_has_answers_per_slot(self):
        doc, _locations, _contents = self.ingested_series()

        for index in (1, 2, 3):
            self.assertTrue(self.db.series_has(doc, SERIES, index))
        self.assertFalse(
            self.db.series_has(doc, SERIES, 4), "the series has three slots"
        )

    def test_series_has_follows_the_gaps_of_a_sparse_series(self):
        doc, _locations, _contents = self.ingested_series(indices=[2, 5, 6])

        for index in (2, 5, 6):
            with self.subTest(present=index):
                self.assertTrue(self.db.series_has(doc, SERIES, index))
        for index in (1, 3, 4, 99):
            with self.subTest(absent=index):
                self.assertFalse(self.db.series_has(doc, SERIES, index))

    def test_series_members_lists_only_the_filled_slots(self):
        doc, _locations, _contents = self.ingested_series(indices=[2, 5, 6])

        indices, uids = self.db.series_members(doc, SERIES)

        self.assertEqual(indices, [2, 5, 6], "the gaps are skipped, not returned")
        self.assertEqual(len(uids), 3)

        manifest = self.manifest_of(doc)
        for index, uid in zip(indices, uids, strict=True):
            self.assertEqual(
                uid,
                manifest["uids"][index - 1],
                "each uid must be the one the manifest gives that slot",
            )

    def test_series_members_uids_resolve_to_the_member_bytes(self):
        """The shape the accessor exists for: one manifest read, then N
        resolutions that are pure functions of a uid."""
        doc, _locations, contents = self.ingested_series()

        indices, uids = self.db.series_members(doc, SERIES)
        self.assertEqual(indices, [1, 2, 3])

        for uid, expected in zip(uids, contents, strict=True):
            path = cached_path_for_uid(uid, additional_roots=[self.db._file_dir()])
            self.assertTrue(path, "the member should be on disk")
            with open(path, "rb") as handle:
                self.assertEqual(handle.read(), expected)

    def test_series_has_is_about_the_manifest_not_the_disk(self):
        """The two questions are deliberately separate: a caller deciding
        what to FETCH needs to know what should be there. Deleting the bytes
        changes exist_doc's answer and must not change this one."""
        doc, _locations, _contents = self.ingested_series()
        _exists, member_path = self.db.exist_doc(doc.id(), "chunkdata.bin_2")
        os.remove(member_path)

        self.assertFalse(
            self.db.exist_doc(doc.id(), "chunkdata.bin_2")[0], "the bytes are gone"
        )
        self.assertTrue(
            self.db.series_has(doc, SERIES, 2),
            "but the series still records the member",
        )

    def test_the_accessors_are_empty_without_a_local_manifest(self):
        """Nothing is fetched to answer, so a manifest that is not here gives
        the same "not here" cached_path_for_file gives."""
        doc, _locations, _contents = self.ingested_series()
        _exists, manifest_path = self.db.exist_doc(doc.id(), SERIES)
        os.remove(manifest_path)

        self.assertFalse(self.db.series_has(doc, SERIES, 1))
        self.assertEqual(self.db.series_members(doc, SERIES), ([], []))

    def test_the_accessors_refuse_an_undeclared_name(self):
        doc, _locations, _contents = self.ingested_series()

        self.assertFalse(
            self.db.series_has(doc, "plainfile.ext", 1),
            "an ordinary file is not a series",
        )
        self.assertEqual(self.db.series_members(doc, "plainfile.ext"), ([], []))
        self.assertFalse(self.db.series_has(doc, "nosuch.bin", 1))

    def test_the_accessors_survive_a_corrupt_manifest(self):
        """An accessor is what a caller walks a level with, so a damaged
        manifest must come back as "nothing here" rather than throwing part
        way through the walk. open_doc is where the corruption is reported,
        once, where it can be acted on."""
        doc, _locations, _contents = self.ingested_series()
        _exists, manifest_path = self.db.exist_doc(doc.id(), SERIES)
        with open(manifest_path, "wb") as handle:
            handle.write(b"NOTAMANIFESTATALL")

        self.assertFalse(self.db.series_has(doc, SERIES, 1))
        self.assertEqual(self.db.series_members(doc, SERIES), ([], []))

    def test_series_has_refuses_a_slot_that_is_not_a_slot(self):
        doc, _locations, _contents = self.ingested_series()

        for index in (0, -1):
            with self.subTest(index=index):
                self.assertFalse(self.db.series_has(doc, SERIES, index))


class TestResolutionReachesNoDatabase(SeriesRoundTripTestCase):
    """The promise a viewer walking a pyramid level depends on."""

    def _series_on_disk_only(self):
        """A document with a series whose manifest and members sit in a
        uid-named root, placed by hand -- no database anywhere."""
        store = os.path.join(self._dir, "store")
        os.makedirs(store, exist_ok=True)
        path = os.path.join(store, "a.bin")
        with open(path, "wb") as handle:
            handle.write(b"the member's bytes")

        doc = Document("demoSeries", **{"demoSeries.value": 1})
        doc.add_file_series(SERIES, [path], delete_original=0)

        root = os.path.join(self._dir, "stubFileDir")
        os.makedirs(root, exist_ok=True)

        _is_in, info, _ = doc.is_in_file_list(SERIES)
        manifest_location = info["locations"][0]["location"]
        with (
            open(manifest_location, "rb") as source,
            open(os.path.join(root, info["locations"][0]["uid"]), "wb") as dest,
        ):
            dest.write(source.read())

        entry = doc.series_ingest_locations(SERIES)[0]
        with (
            open(entry["location"], "rb") as source,
            open(os.path.join(root, entry["uid"]), "wb") as dest,
        ):
            dest.write(source.read())

        return doc, root, entry["uid"]

    def test_cached_path_for_file_resolves_a_member_with_no_query(self):
        doc, root, member_uid = self._series_on_disk_only()

        exists, path = NoQueryDatabase([root]).cached_path_for_file(
            doc, "chunkdata.bin_1"
        )

        self.assertTrue(exists)
        self.assertEqual(path, os.path.join(root, member_uid))

    def test_the_series_accessors_reach_no_database(self):
        doc, root, member_uid = self._series_on_disk_only()
        db = NoQueryDatabase([root])

        self.assertTrue(db.series_has(doc, SERIES, 1))
        self.assertEqual(db.series_members(doc, SERIES), ([1], [member_uid]))

    def test_the_stub_database_really_would_refuse(self):
        """Guard the premise: if NoQueryDatabase answered instead of raising,
        the three tests above would prove nothing."""
        with self.assertRaises(AssertionError):
            NoQueryDatabase([]).all_doc_ids()


class TestReAddingAStoredSeries(SeriesRoundTripTestCase):
    """The cloud shape NDI-python#215 is about: a document comes back from a
    store with its member paths stripped, and something wants to put it into
    another database. The guard (#185) refuses that outright; this is what
    the refusal's own remedy -- re-populating the locations -- actually
    takes."""

    def _other_db(self):
        directory = os.path.join(self._dir, "other")
        os.makedirs(directory, exist_ok=True)
        db = SQLiteDB(os.path.join(directory, "db.sqlite"))
        db.add_branch("a")
        db.set_branch("a")
        self.addCleanup(db._close_db)
        return db

    def test_restoring_the_member_locations_is_not_enough_on_its_own(self):
        """The manifest is an ordinary file with an ordinary location, and
        add_file_series wrote it to a temporary path that ingestion consumed.
        So a stored document needs its MANIFEST located again as well as its
        members -- the guard's message names only the members, which is worth
        knowing before writing a caller against it."""
        doc, _locations, contents = self.ingested_series(count=2)
        members = [dict(e) for e in doc.series_ingest_locations(SERIES)]
        stored = self.db.get_docs(doc.id())
        stored.document_properties["files"]["series_info"][0][
            "ingest_locations"
        ] = members

        other = self._other_db()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            other.add_docs([stored], validate=False)

        self.assertEqual(
            other.all_doc_ids(), [stored.id()], "the guard is satisfied and it lands"
        )
        self.assertTrue(
            any("chunkdata.bin" in str(w.message) for w in caught),
            "and the manifest it could not ingest is at least warned about",
        )
        for index, expected in enumerate(contents, start=1):
            with self.subTest(member=index):
                path = os.path.join(other._file_dir(), self.member_uids[index - 1])
                with open(path, "rb") as handle:
                    self.assertEqual(handle.read(), expected)
        self.assertFalse(
            other.exist_doc(stored.id(), "chunkdata.bin_1")[0],
            "the members are here, but without the manifest nothing maps a "
            "slot to them",
        )

    def test_with_the_manifest_located_too_the_whole_series_comes_across(self):
        doc, _locations, contents = self.ingested_series(count=2)
        members = [dict(e) for e in doc.series_ingest_locations(SERIES)]
        stored = self.db.get_docs(doc.id())
        stored.document_properties["files"]["series_info"][0][
            "ingest_locations"
        ] = members

        # Point the manifest's location at the copy this database ingested.
        _exists, manifest_path = self.db.exist_doc(doc.id(), SERIES)
        _is_in, info, _ = stored.is_in_file_list(SERIES)
        info["locations"] = [
            dict(info["locations"][0], location=manifest_path, delete_original=0)
        ]

        other = self._other_db()
        other.add_docs([stored], validate=False)

        for index, expected in enumerate(contents, start=1):
            name = f"{SERIES}_{index}"
            with self.subTest(member=name):
                exists, path = other.exist_doc(stored.id(), name)
                self.assertTrue(exists)
                with open(path, "rb") as handle:
                    self.assertEqual(handle.read(), expected)

        self.assertEqual(other.series_members(stored, SERIES)[0], [1, 2])


if __name__ == "__main__":
    unittest.main()
