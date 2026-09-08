"""Document-level file series: declaration, authoring, enumeration.

Ports DID-matlab's ``tests/+did/+unittest/TestDocumentFileSeries.m``. See
VH-Lab/DID-matlab#173. The manifest format itself is covered by
``tests/symmetry/make_artifacts/file/test_series_manifest.py``; this covers
``did.document``'s use of it.

A file series is a ``NAME_#`` family whose members are indexed by a single
binary manifest rather than by one ``file_info`` struct each. ``NAME`` itself
is an ordinary file of the document -- the manifest -- and the members are
``NAME_1`` ... ``NAME_N``. So there are two index bases in play: members are
ONE-based, as written in the name, and the manifest's array is ZERO-based.
The conversion is the boundary these tests spend the most effort on.

MATLAB raises identified errors (``DID:Document:addFileSeries:notDeclared``
and friends); Python's ``did.document`` raises plain ``ValueError`` for
document-level problems, as ``add_file_series`` already did before these
tests existed. The refusals are matched on which input is refused, not on an
identifier.

Two refusals from the same MATLAB commit had no Python counterpart when this
file was first written, and were left untested rather than pinned as absent:
the constructor's file-declaration validation, and ``add_file``'s refusal of
a series-member name. Both are ported now (DID-python#69) and covered by
``TestFileDeclarationValidation`` and ``TestAddFileRefusesASeriesMember``.
One deliberate divergence remains inside the first, over which trailing
parts count as a member's index; it is pinned and explained there.
"""

import copy
import os
import tempfile
import unittest

from did.document import Document
from did.file import read_series_manifest
from did.ido import IDO

SERIES = "chunkdata.bin"


class SeriesTestCase(unittest.TestCase):
    """Shared scaffolding: a scratch tree to put members in."""

    def setUp(self):
        self._dir = tempfile.mkdtemp()

    def write_member(self, folder, name):
        """Write a member's bytes under ``folder`` and return its path."""
        directory = os.path.join(self._dir, folder) if folder else self._dir
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, name)
        with open(path, "wb") as handle:
            handle.write(bytes(range(10)))
        return path

    def under(self, *parts):
        return os.path.join(self._dir, *parts)

    def manifest_location(self, doc, name=SERIES):
        """Where add_file_series left the manifest.

        add_file records the location and does not move it until add_docs
        runs, so the file is still there for a test to read.
        """
        is_in, info, _ = doc.is_in_file_list(name)
        self.assertTrue(is_in, "the manifest was not added as a file")
        return info["locations"][0]["location"]

    def manifest(self, doc, name=SERIES):
        return read_series_manifest(self.manifest_location(doc, name))


class TestSeriesDeclaration(SeriesTestCase):
    def test_series_names_come_from_the_class_definition(self):
        doc = Document("demoSeries")
        self.assertEqual(doc.series_names(), [SERIES])
        self.assertTrue(doc.is_file_series(SERIES))
        self.assertTrue(
            doc.is_file_series("CHUNKDATA.BIN"),
            "matching must be case-insensitive, as is_in_file_list is",
        )
        self.assertFalse(doc.is_file_series("plainfile.ext"))

    def test_a_class_with_no_series_has_none(self):
        doc = Document("demoFile", **{"demoFile.value": 1})
        self.assertEqual(doc.series_names(), [])
        self.assertFalse(doc.is_file_series("filename1.ext"))

    def test_count_is_zero_before_the_series_is_added(self):
        doc = Document("demoSeries")
        self.assertEqual(doc.series_count(SERIES), (0, 0))

    def test_a_single_declared_name_is_still_a_list(self):
        """MATLAB stores one series as a char row, not a cell; JSON round
        trips can produce the same shape here."""
        doc = Document("demoSeries")
        doc.document_properties["files"]["file_series"] = SERIES
        self.assertEqual(doc.series_names(), [SERIES])
        self.assertTrue(doc.is_file_series(SERIES))


class TestSeriesAuthoring(SeriesTestCase):
    def test_a_dense_series_records_every_slot(self):
        locations = [self.write_member("store", n) for n in ("a", "b", "c")]

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations)

        self.assertEqual(doc.series_count(SERIES), (3, 3))

    def test_add_file_series_returns_the_document(self):
        doc = Document("demoSeries")
        self.assertIs(
            doc.add_file_series(SERIES, [self.write_member("store", "a")]), doc
        )

    def test_a_sparse_series_records_slots_not_just_members(self):
        """The case a NAME_# entry cannot express: members 1, 3 and 7 of a
        seven-slot series, with nothing written for the gaps."""
        locations = [self.write_member("store", n) for n in ("a", "b", "c")]

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations, indices=[1, 3, 7])

        count, n_present = doc.series_count(SERIES)
        self.assertEqual(count, 7, "count is the number of SLOTS")
        self.assertEqual(n_present, 3, "n_present is what exists")

    def test_members_land_in_the_right_manifest_slots(self):
        """Members are ONE-based; the manifest array is ZERO-based. This pins
        the conversion, which is the only place the two differ."""
        locations = [self.write_member("store", n) for n in ("a", "b")]

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations, indices=[1, 4])

        manifest = self.manifest(doc)
        self.assertEqual(manifest["count"], 4)
        self.assertTrue(manifest["uids"][0], "member 1 -> array slot 0")
        self.assertEqual(manifest["uids"][1], "")
        self.assertEqual(manifest["uids"][2], "")
        self.assertTrue(manifest["uids"][3], "member 4 -> array slot 3")
        self.assertNotEqual(manifest["uids"][0], manifest["uids"][3])

    def test_every_member_gets_its_own_uid(self):
        locations = [self.write_member("store", n) for n in ("a", "b", "c")]

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations)

        uids = self.manifest(doc)["uids"]
        self.assertEqual(len(set(uids)), 3)
        self.assertTrue(all(uids))

    def test_the_manifest_is_recorded_as_an_ordinary_file(self):
        """The series name IS its manifest: it gets a file_info entry and a
        uid like any other file, which is how everything downstream finds
        it."""
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, [self.write_member("store", "a")])

        self.assertEqual(len(doc.file_uids(SERIES)), 1)
        is_in, info, _ = doc.is_in_file_list(SERIES)
        self.assertTrue(is_in)
        self.assertEqual(info["locations"][0]["uid"], doc.file_uids(SERIES)[0])
        self.assertTrue(os.path.isfile(self.manifest_location(doc)))

    def test_source_names_are_relative_to_the_derived_root(self):
        """Absolute paths are never recorded: a full path exposes a directory
        layout as soon as a document is shared."""
        locations = [
            self.write_member(os.path.join("store", "0"), "a"),
            self.write_member(os.path.join("store", "1"), "b"),
        ]

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations)

        self.assertEqual(doc.series_source_root(SERIES), self.under("store"))

        manifest = self.manifest(doc)
        self.assertTrue(manifest["has_source_names"])
        self.assertEqual(
            manifest["source_names"],
            ["0/a", "1/b"],
            "relative, and separated with / on every platform",
        )

    def test_an_explicit_source_root_overrides_the_derived_one(self):
        """Deriving would give the member's own directory, so the recorded
        name would be just 'a'. Passing the root explicitly is how a caller
        says where the tree starts rather than where the file sits."""
        location = self.write_member(os.path.join("store", "level0"), "a")

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, [location], source_root=self.under("store"))

        self.assertEqual(doc.series_source_root(SERIES), self.under("store"))
        self.assertEqual(
            self.manifest(doc)["source_names"],
            ["level0/a"],
            "relative to the root that was given, not to a derived one",
        )

    def test_a_location_outside_an_explicit_root_is_refused(self):
        """Recording it would put an absolute path in the manifest, which is
        the directory-layout disclosure relative names exist to prevent."""
        locations = [
            self.write_member("store", "a"),
            self.write_member("elsewhere", "b"),
        ]

        doc = Document("demoSeries")
        with self.assertRaises(ValueError) as caught:
            doc.add_file_series(SERIES, locations, source_root=self.under("store"))
        self.assertIn("not under the series source root", str(caught.exception))

    def test_a_refused_add_leaves_no_half_series_behind(self):
        """The refusal above happens before the manifest is written, so the
        document must be as it was -- otherwise a caller that caught the
        error would hold a document claiming a series it has not got."""
        locations = [
            self.write_member("store", "a"),
            self.write_member("elsewhere", "b"),
        ]

        doc = Document("demoSeries")
        with self.assertRaises(ValueError):
            doc.add_file_series(SERIES, locations, source_root=self.under("store"))

        self.assertEqual(doc.series_count(SERIES), (0, 0))
        self.assertEqual(doc.series_ingest_locations(SERIES), [])
        self.assertEqual(doc.file_uids(SERIES), [], "no manifest was recorded")

    def test_uid_width_reaches_the_manifest(self):
        """A pass-through to did.file.write_series_manifest, but a dropped one
        would write a header whose stride disagrees with the reader's. A
        did.ido uid is 33 characters, so this widens rather than narrows."""
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, [self.write_member("store", "a")], uid_width=40)

        manifest = self.manifest(doc)
        self.assertEqual(manifest["uid_width"], 40)
        self.assertEqual(
            len(manifest["uids"][0]),
            33,
            "the uid itself is unchanged; only the slot is wider",
        )

    def test_source_names_can_be_declined(self):
        doc = Document("demoSeries")
        doc.add_file_series(
            SERIES, [self.write_member("store", "a")], record_source_names=False
        )

        self.assertEqual(doc.series_source_root(SERIES), "")
        self.assertFalse(self.manifest(doc)["has_source_names"])

    def test_members_with_no_common_root_record_no_source_names(self):
        """Nothing in common but the filesystem root, so there is no root
        worth recording and the alternative -- absolute paths -- is the
        disclosure the relative form exists to avoid.

        The paths are fictional on purpose: add_file_series never opens a
        member, so root derivation is pure string work.
        """
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, ["/aaa/one/x", "/bbb/two/y"])

        self.assertEqual(doc.series_source_root(SERIES), "")
        self.assertEqual(
            doc.series_count(SERIES),
            (2, 2),
            "membership is still recorded; only the names are dropped",
        )

        manifest = self.manifest(doc)
        self.assertFalse(manifest["has_source_names"])
        self.assertTrue(manifest["uids"][0])
        self.assertTrue(manifest["uids"][1])

    def test_url_members_are_given_no_root(self):
        """A URL carries no home directory to leak and is stored whole, so it
        gets no root and no relative name."""
        doc = Document("demoSeries")
        doc.add_file_series(
            SERIES, ["https://example.org/store/a", "https://example.org/store/b"]
        )

        self.assertEqual(doc.series_source_root(SERIES), "")
        self.assertFalse(self.manifest(doc)["has_source_names"])

    def test_members_in_different_subtrees_are_still_recorded(self):
        """Deliberately NOT claiming "no common root": two paths under one
        temporary directory always share one. What this pins is that members
        spread across subtrees are all recorded, whatever root is derived."""
        locations = [
            self.write_member("one", "a"),
            self.write_member("two", "b"),
        ]

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations)

        manifest = self.manifest(doc)
        self.assertEqual(manifest["count"], 2, "the members are still recorded")
        self.assertTrue(manifest["uids"][0])
        self.assertTrue(manifest["uids"][1])


class TestSeriesIngestLocations(SeriesTestCase):
    """Transient records of where each member's bytes currently sit, so that
    ingestion can copy them. Stripped before the document's JSON is stored.
    See VH-Lab/DID-matlab#173."""

    def test_every_present_member_is_recorded(self):
        locations = [self.write_member("store", n) for n in ("a", "b")]

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations)

        entries = doc.series_ingest_locations(SERIES)
        self.assertEqual(len(entries), 2)
        self.assertEqual([e["index"] for e in entries], [1, 2])
        self.assertEqual([e["location"] for e in entries], locations)
        self.assertEqual([e["location_type"] for e in entries], ["file", "file"])
        self.assertEqual([e["ingest"] for e in entries], [1, 1])

    def test_ingest_uids_match_the_manifest_slots(self):
        """The pairing is the whole point: ingestion copies location -> uid,
        and the manifest is what later resolves NAME_i -> uid. If the two
        disagreed, a member would be stored under a uid nothing looks up."""
        locations = [self.write_member("store", n) for n in ("a", "b")]

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations, indices=[2, 5])

        entries = doc.series_ingest_locations(SERIES)
        manifest = self.manifest(doc)

        self.assertEqual([e["index"] for e in entries], [2, 5])
        for entry in entries:
            self.assertEqual(
                entry["uid"],
                manifest["uids"][entry["index"] - 1],
                "the ingest uid must be the uid the manifest gives that slot",
            )

    def test_a_sparse_series_records_only_present_members(self):
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, [self.write_member("store", "a")], indices=[4])

        self.assertEqual(doc.series_count(SERIES), (4, 1))
        entries = doc.series_ingest_locations(SERIES)
        self.assertEqual(len(entries), 1, "absent slots have no bytes to ingest")
        self.assertEqual(entries[0]["index"], 4)

    def test_locations_survive_when_source_names_are_declined(self):
        """recordSourceNames=False must drop the provenance and nothing else;
        without the locations the series could never be ingested at all."""
        location = self.write_member("store", "a")

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, [location], record_source_names=False)

        self.assertEqual(
            doc.series_source_root(SERIES), "", "no provenance is recorded, as asked"
        )
        entries = doc.series_ingest_locations(SERIES)
        self.assertEqual(len(entries), 1)
        self.assertEqual(
            entries[0]["location"],
            location,
            "but ingestion still knows where the member is",
        )

    def test_url_members_are_not_ingested_and_keep_their_original(self):
        """Mirrors add_file: a URL is a reference, not something to copy in
        and then delete."""
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, ["https://example.org/store/a"])

        entry = doc.series_ingest_locations(SERIES)[0]
        self.assertEqual(entry["location_type"], "url")
        self.assertEqual(entry["ingest"], 0)
        self.assertEqual(entry["delete_original"], 0)

    def test_delete_original_defaults_per_type_and_can_be_overridden(self):
        locations = [self.write_member("store", "a")]

        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations)
        self.assertEqual(
            doc.series_ingest_locations(SERIES)[0]["delete_original"],
            1,
            "a local file follows add_file, which deletes the original",
        )

        other = Document("demoSeries")
        other.add_file_series(SERIES, locations, delete_original=0)
        self.assertEqual(
            other.series_ingest_locations(SERIES)[0]["delete_original"],
            0,
            "and a caller with 28,000 members can say no",
        )

    def test_ingest_can_be_overridden_too(self):
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, [self.write_member("store", "a")], ingest=0)
        self.assertEqual(doc.series_ingest_locations(SERIES)[0]["ingest"], 0)

    def test_ingest_locations_are_empty_before_the_series_is_added(self):
        doc = Document("demoSeries")
        self.assertEqual(doc.series_ingest_locations(SERIES), [])
        self.assertEqual(doc.series_ingest_locations("nosuch.bin"), [])

    def test_source_root_is_empty_for_an_unadded_series(self):
        doc = Document("demoSeries")
        self.assertEqual(doc.series_source_root(SERIES), "")
        self.assertEqual(doc.series_source_root("nosuch.bin"), "")


class TestSeriesStripping(SeriesTestCase):
    def _added(self, *names):
        locations = [self.write_member("store", n) for n in (names or ("a",))]
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations)
        return doc, locations

    def test_strip_removes_locations_and_keeps_the_rest(self):
        doc, _ = self._added()

        props = Document.strip_series_ingest_locations(
            copy.deepcopy(doc.document_properties)
        )
        entry = props["files"]["series_info"][0]

        self.assertIn(
            "ingest_locations",
            entry,
            "the field stays, so a stored document keeps a fresh one's shape",
        )
        self.assertEqual(entry["ingest_locations"], [], "but carries nothing")
        self.assertEqual(entry["name"], SERIES)
        self.assertEqual(entry["count"], 1)
        self.assertEqual(entry["n_present"], 1)
        self.assertEqual(
            entry["source_root"],
            self.under("store"),
            "provenance is kept; only the pending paths go",
        )

    def test_stripped_properties_carry_no_member_paths(self):
        """What the database stores must not contain a member path at all."""
        import json

        doc, locations = self._added("a", "b")

        props = Document.strip_series_ingest_locations(
            copy.deepcopy(doc.document_properties)
        )
        encoded = json.dumps(props)
        for location in locations:
            self.assertNotIn(
                location, encoded, "a member path must not reach the stored JSON"
            )

    def test_strip_is_idempotent(self):
        doc, _ = self._added()

        once = Document.strip_series_ingest_locations(
            copy.deepcopy(doc.document_properties)
        )
        twice = Document.strip_series_ingest_locations(copy.deepcopy(once))
        self.assertEqual(twice, once)

    def test_strip_leaves_a_document_with_no_series_alone(self):
        """Removing the field instead of emptying it would leave a stored
        document one field short of a fresh one, and adding a series to it
        would then fail because the shapes disagree."""
        plain = Document("demoFile", **{"demoFile.value": 1})
        before = copy.deepcopy(plain.document_properties)

        after = Document.strip_series_ingest_locations(plain.document_properties)
        self.assertEqual(after, before)

    def test_strip_accepts_things_that_are_not_documents(self):
        self.assertEqual(Document.strip_series_ingest_locations({}), {})
        self.assertEqual(
            Document.strip_series_ingest_locations({"files": {}}), {"files": {}}
        )
        self.assertIsNone(Document.strip_series_ingest_locations(None))

    def test_accessors_are_empty_on_a_stored_document(self):
        """The round trip a reader takes: the database stores the stripped
        properties and hands them back, and Document(dict) rebuilds from
        them."""
        doc, _ = self._added()

        stored = Document(
            Document.strip_series_ingest_locations(
                copy.deepcopy(doc.document_properties)
            )
        )

        self.assertEqual(
            stored.series_ingest_locations(SERIES),
            [],
            "a stored document no longer says where its members came from",
        )
        self.assertEqual(
            stored.series_count(SERIES),
            (1, 1),
            "but it still knows the series and its size",
        )

    def test_a_series_can_be_added_to_a_stored_document(self):
        """The latent bug emptying rather than removing guards against: a
        document that has been stored comes back stripped, and adding a
        series to it must still work."""
        doc, _ = self._added()

        stored = Document(
            Document.strip_series_ingest_locations(
                copy.deepcopy(doc.document_properties)
            )
        )
        stored.remove_file_series(SERIES)

        second = self.write_member("store", "b")
        stored.add_file_series(SERIES, [second])

        entries = stored.series_ingest_locations(SERIES)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["location"], second)


class TestFileDeclarationValidation(SeriesTestCase):
    """A name must be served by ONE mechanism, not two.

    These four refusals run in the CONSTRUCTOR rather than in
    add_file_series, so they are reached by building the properties dict
    directly. Doing it that way also keeps deliberately malformed
    declarations out of the example schema, where a future reader would have
    to work out whether they were broken on purpose.

    Ported from MATLAB's localValidateFileDeclarations; they had no Python
    counterpart until DID-python#69. MATLAB raises four identified errors
    here and Python raises ValueError, as the rest of this layer does, so
    each is matched on which input is refused and on the message.
    """

    def declaration(self, file_list, file_series):
        """A minimal properties dict carrying just the file declarations."""
        return {
            "base": {"id": IDO.unique_id(), "name": "test"},
            "files": {"file_list": file_list, "file_series": file_series},
        }

    def test_a_valid_declaration_constructs(self):
        """The positive control. Without it, a validator that rejected
        everything would pass all four refusals below."""
        doc = Document(self.declaration(["plainfile.ext", SERIES], [SERIES]))
        self.assertEqual(doc.series_names(), [SERIES])

    def test_the_shipped_class_still_constructs(self):
        """The other positive control, and the one that matters in practice:
        the real demoSeries definition must pass its own validator."""
        self.assertEqual(Document("demoSeries").series_names(), [SERIES])

    def test_a_series_must_also_be_in_the_file_list(self):
        """The series name IS its manifest, and a manifest is an ordinary
        file, so it has to be declared as one."""
        with self.assertRaises(ValueError) as caught:
            Document(self.declaration(["plainfile.ext"], [SERIES]))
        self.assertIn("not in file_list", str(caught.exception))

    def test_a_series_beside_a_numbered_entry_is_refused(self):
        """ "chunkdata.bin_12" would match both mechanisms, and they would
        disagree: the probe path stops at the first gap, which is the case
        series exist to serve."""
        with self.assertRaises(ValueError) as caught:
            Document(self.declaration([SERIES, f"{SERIES}_#"], [SERIES]))
        self.assertIn("would match both", str(caught.exception))

    def test_a_literal_entry_shadowed_by_a_series_is_refused(self):
        """A trailing integer resolves through the series first, so this
        literal entry could never be reached."""
        with self.assertRaises(ValueError) as caught:
            Document(self.declaration([SERIES, f"{SERIES}_3"], [SERIES]))
        self.assertIn("unreachable", str(caught.exception))

    def test_a_duplicate_series_name_is_refused(self):
        """Case-insensitive, because membership is matched that way and a
        difference the matching cannot see is not a difference."""
        with self.assertRaises(ValueError) as caught:
            Document(self.declaration([SERIES], [SERIES, SERIES.upper()]))
        self.assertIn("more than once", str(caught.exception))

    def test_the_check_runs_on_a_document_rebuilt_from_properties(self):
        """Document(dict) is how a stored document comes back, and it is the
        only route by which a malformed declaration reaches this at all --
        so the check cannot live on the class-definition branch alone."""
        with self.assertRaises(ValueError):
            Document(self.declaration(["plainfile.ext"], [SERIES]))

    def test_a_class_declaring_no_series_is_unaffected(self):
        """The validator returns early on the common case; a numbered entry
        with no series to shadow it is ordinary."""
        doc = Document(self.declaration(["plainfile.ext", "chunk_3"], []))
        self.assertEqual(doc.series_names(), [])

    def test_a_single_declared_series_name_is_accepted_as_a_string(self):
        """MATLAB stores one series as a char row rather than a cell, and a
        JSON round trip can produce the same shape here."""
        doc = Document(self.declaration([SERIES], SERIES))
        self.assertEqual(doc.series_names(), [SERIES])

    def test_a_name_python_does_not_parse_as_a_member_is_not_shadowed(self):
        """A DELIBERATE DIVERGENCE from MATLAB, pinned so it is not mistaken
        for an oversight.

        MATLAB catches this case too, because its is_in_file_list resolves
        the trailing part with str2num, which EVALUATES -- so "_pi" and "_i"
        parse as numbers there and reach the series path. Python's
        series_member_of requires str.isdigit(), so "chunkdata.bin_pi" is
        not a member here, nothing shadows the literal entry, and refusing
        it would refuse a declaration Python resolves unambiguously.

        The guard has to match THIS language's resolution rule, or it would
        be enforcing a collision that does not exist. The cost is a
        portability gap in one direction: such a declaration is valid here
        and would be refused by MATLAB. Emulating str2num -- which evaluates
        arbitrary expressions -- is not a reasonable way to close it. See
        DID-python#69.
        """
        doc = Document(self.declaration([SERIES, f"{SERIES}_pi"], [SERIES]))

        self.assertEqual(doc.series_names(), [SERIES])
        self.assertEqual(
            doc.series_member_of(f"{SERIES}_pi"),
            ("", None),
            "the premise: Python's series path does not claim this name",
        )


class TestAddFileRefusesASeriesMember(SeriesTestCase):
    """add_file must not give a member an inline file_info entry: the
    manifest would not know about it, so file_uids would answer for a member
    series_members and series_count have never heard of."""

    def test_a_member_name_cannot_be_added_directly(self):
        doc = Document("demoSeries")

        with self.assertRaises(ValueError) as caught:
            doc.add_file("chunkdata.bin_5", self.write_member("store", "a"))
        self.assertIn("member of the file series", str(caught.exception))
        self.assertEqual(doc.file_uids("chunkdata.bin_5"), [])

    def test_the_series_name_itself_is_still_addable(self):
        """The manifest is an ordinary file, and add_file_series adds it
        through this very method -- so the guard must not catch it."""
        doc = Document("demoSeries")

        doc.add_file(SERIES, self.write_member("store", "manifest"))

        self.assertEqual(len(doc.file_uids(SERIES)), 1)

    def test_an_ordinary_numbered_name_is_still_addable(self):
        """plainfile.ext is not a series, so plainfile.ext_1 is nobody's
        member and the guard must leave it alone."""
        doc = Document("demoSeries")

        doc.add_file("plainfile.ext_1", self.write_member("store", "a"))

        self.assertEqual(len(doc.file_uids("plainfile.ext_1")), 1)

    def test_a_class_with_no_series_is_unaffected(self):
        doc = Document("demoFile", **{"demoFile.value": 1})

        doc.add_file("filename1.ext_1", self.write_member("store", "a"))

        self.assertEqual(len(doc.file_uids("filename1.ext_1")), 1)


class TestSeriesRefusals(SeriesTestCase):
    def test_an_undeclared_series_is_refused(self):
        doc = Document("demoSeries")
        with self.assertRaises(ValueError) as caught:
            doc.add_file_series("nosuch.bin", ["x"])
        self.assertIn("not declared as a file series", str(caught.exception))

    def test_a_class_with_no_series_at_all_refuses(self):
        doc = Document("demoFile", **{"demoFile.value": 1})
        with self.assertRaises(ValueError):
            doc.add_file_series("filename1.ext", ["x"])

    def test_adding_twice_is_refused(self):
        locations = [self.write_member("store", "a")]
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations)

        with self.assertRaises(ValueError) as caught:
            doc.add_file_series(SERIES, locations)
        self.assertIn("has already been added", str(caught.exception))

    def test_a_zero_or_negative_index_is_refused(self):
        """Members are one-based, matching the live NAME_# convention. A zero
        index is a caller assuming the other base and must not pass
        silently."""
        locations = [self.write_member("store", "a")]
        doc = Document("demoSeries")

        for bad in (0, -1):
            with self.subTest(index=bad), self.assertRaises(ValueError):
                doc.add_file_series(SERIES, locations, indices=[bad])

    def test_a_non_integer_index_is_refused(self):
        locations = [self.write_member("store", "a")]
        doc = Document("demoSeries")
        with self.assertRaises(ValueError):
            doc.add_file_series(SERIES, locations, indices=[1.5])

    def test_mismatched_index_and_location_counts_are_refused(self):
        locations = [self.write_member("store", n) for n in ("a", "b")]
        doc = Document("demoSeries")

        with self.assertRaises(ValueError) as caught:
            doc.add_file_series(SERIES, locations, indices=[1])
        self.assertIn("locations", str(caught.exception))

    def test_duplicate_indices_are_refused(self):
        locations = [self.write_member("store", n) for n in ("a", "b")]
        doc = Document("demoSeries")

        with self.assertRaises(ValueError) as caught:
            doc.add_file_series(SERIES, locations, indices=[2, 2])
        self.assertIn("unique", str(caught.exception))


class TestSeriesMemberOf(SeriesTestCase):
    """series_member_of is the public form of the membership rule. A database
    has to ask it too -- a member has no files-table row, so resolving one
    starts by deciding whether the name is a member at all."""

    def test_it_names_the_series_and_the_slot(self):
        doc = Document("demoSeries")

        stem, index = doc.series_member_of("chunkdata.bin_7")
        self.assertEqual(stem, SERIES)
        self.assertEqual(index, 7, "indices are one-based, as written in the name")

    def test_it_returns_the_declared_spelling(self):
        """Matching is case-insensitive, but everything downstream looks the
        stem up again -- in file_info, in the files table -- where the
        declared spelling is what is stored."""
        doc = Document("demoSeries")

        stem, index = doc.series_member_of("CHUNKDATA.BIN_2")
        self.assertEqual(stem, SERIES)
        self.assertEqual(index, 2)

    def test_it_says_no_rather_than_erroring(self):
        doc = Document("demoSeries")

        for name in (
            SERIES,
            "plainfile.ext_1",
            "nosuch.bin_5",
            "nounderscore",
            "chunkdata.bin_",
            "chunkdata.bin_x",
            "chunkdata.bin_1.5",
            "",
            None,
            42,
        ):
            with self.subTest(name=name):
                stem, index = doc.series_member_of(name)
                self.assertEqual(stem, "")
                self.assertIsNone(index)

    def test_it_is_empty_for_a_class_with_no_series(self):
        doc = Document("demoFile", **{"demoFile.value": 1})
        self.assertEqual(doc.series_member_of("filename1.ext_1"), ("", None))

    def test_is_series_member_is_the_boolean_form(self):
        doc = Document("demoSeries")
        self.assertTrue(doc.is_series_member("chunkdata.bin_5"))
        self.assertFalse(doc.is_series_member(SERIES))
        self.assertFalse(doc.is_series_member("nosuch.bin_5"))

    def test_membership_does_not_depend_on_the_series_being_populated(self):
        """A member name resolves from the DECLARATION alone. Nothing has to
        be added for a reader to ask whether a name is a member."""
        doc = Document("demoSeries")
        self.assertEqual(doc.series_count(SERIES), (0, 0))
        self.assertTrue(doc.is_series_member("chunkdata.bin_1"))


class TestSeriesRemoval(SeriesTestCase):
    def test_remove_clears_the_record_and_allows_re_adding(self):
        locations = [self.write_member("store", "a")]
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, locations)

        doc.remove_file_series(SERIES)

        self.assertEqual(doc.series_count(SERIES), (0, 0))
        self.assertTrue(
            doc.is_file_series(SERIES),
            "the DECLARATION belongs to the class and survives removal",
        )

        doc.add_file_series(SERIES, locations)
        self.assertEqual(doc.series_count(SERIES), (1, 1))

    def test_remove_drops_the_manifest_file_entry(self):
        """Otherwise re-adding would append a second location to the old
        manifest's file_info entry, and the stale one is listed first -- so
        every later lookup would resolve to the manifest that was removed."""
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, [self.write_member("store", "a")])
        first_manifest_uid = doc.file_uids(SERIES)[0]

        doc.remove_file_series(SERIES)
        self.assertEqual(doc.file_uids(SERIES), [])

        doc.add_file_series(SERIES, [self.write_member("store", "b")])
        uids = doc.file_uids(SERIES)
        self.assertEqual(len(uids), 1)
        self.assertNotEqual(uids[0], first_manifest_uid)

    def test_remove_leaves_other_files_alone(self):
        doc = Document("demoSeries")
        doc.add_file("plainfile.ext", self.write_member("store", "plain"))
        doc.add_file_series(SERIES, [self.write_member("store", "a")])

        doc.remove_file_series(SERIES)

        self.assertEqual(len(doc.file_uids("plainfile.ext")), 1)

    def test_removing_an_unadded_series_is_an_error(self):
        doc = Document("demoSeries")
        with self.assertRaises(ValueError) as caught:
            doc.remove_file_series(SERIES)
        self.assertIn("has not been added", str(caught.exception))

    def test_remove_returns_the_document(self):
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, [self.write_member("store", "a")])
        self.assertIs(doc.remove_file_series(SERIES), doc)


class TestFileUids(SeriesTestCase):
    """file_uids is the in-memory half of resolving a file, and the way every
    series lookup finds its manifest. It arrived with the series work and had
    no test of its own."""

    def test_it_returns_the_uids_add_file_recorded_in_order(self):
        doc = Document("demoSeries")
        doc.add_file("plainfile.ext", self.write_member("store", "one"))
        doc.add_file("plainfile.ext", "https://example.org/one")

        uids = doc.file_uids("plainfile.ext")
        self.assertEqual(len(uids), 2)
        self.assertEqual(len(set(uids)), 2)

        _is_in, info, _ = doc.is_in_file_list("plainfile.ext")
        self.assertEqual([loc["uid"] for loc in info["locations"]], uids)

    def test_it_is_empty_for_a_file_the_document_does_not_have(self):
        doc = Document("demoSeries")
        self.assertEqual(doc.file_uids("plainfile.ext"), [])
        self.assertEqual(doc.file_uids("nosuch.ext"), [])

    def test_it_is_empty_for_a_series_member(self):
        """A member has no file_info entry by design -- membership is the
        manifest's to answer -- so the miss here is what sends
        Database.cached_path_for_file down the series path."""
        doc = Document("demoSeries")
        doc.add_file_series(SERIES, [self.write_member("store", "a")])

        self.assertEqual(doc.file_uids("chunkdata.bin_1"), [])
        self.assertEqual(len(doc.file_uids(SERIES)), 1, "but the manifest has one")

    def test_it_matches_case_insensitively(self):
        doc = Document("demoSeries")
        doc.add_file("plainfile.ext", self.write_member("store", "one"))
        self.assertEqual(doc.file_uids("PLAINFILE.EXT"), doc.file_uids("plainfile.ext"))


if __name__ == "__main__":
    unittest.main()
