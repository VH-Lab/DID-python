"""makeArtifact symmetry test: a database holding an ingested file series.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+makeArtifacts/+database/fileSeriesDocument.m.

The fileDocument pair covers a document whose files each have a files-table
row. A SERIES member deliberately has none: it is resolved through the
manifest, and reproducing that is the whole point of the feature
(VH-Lab/DID-matlab#173). A port could make member reads "work" by giving each
member a row and would pass a naive read-back test while putting back the
~8-11 MB of per-member JSON the manifest exists to remove. So this artifact
pins the ABSENCE as hard as it pins the bytes.

Four things are recorded here that a port has to reproduce, and that nothing
else in the symmetry suite would catch:

  1. members land at <FileDir>/<uid>, under the uid the manifest gives that
     slot -- the pairing the ingest side and the read side share;
  2. the files table holds ONE row, the manifest's, and no member's;
  3. the stored document JSON keeps files.series_info with ingest_locations
     present but EMPTY, not removed. DID-matlab used rmfield here and it was
     wrong (commit 87ebcf6): a stored document then had a narrower struct
     than a fresh one, and adding a series to it failed at runtime much
     later. A port using ``del`` instead of an empty list rediscovers exactly
     that;
  4. NAME_<i> is ONE-based in a document and ZERO-based in the manifest. The
     series here is SPARSE, so an off-by-one resolves a present member to a
     gap or to its neighbour rather than merely shifting everything.

Deliberately no custom_file_handler and nothing remote: retrieval needs a
live handler and cannot be a static artifact. What is checkable without one
is the layout the handler would place bytes into, which is what this records.

Artifacts are written to:
    <tempdir>/DID/symmetryTest/pythonArtifacts/database/fileSeriesDocument/testFileSeriesDocumentArtifacts/
"""

import json
import os
import shutil

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from tests.symmetry.conftest import PYTHON_ARTIFACTS

DB_FILENAME = "file_series_document_test.sqlite"
BRANCH = "branch_main"
# demoSeries declares 'chunkdata.bin' as a series and 'plainfile.ext' as an
# ordinary file.
SERIES_NAME = "chunkdata.bin"
# A sparse series: six slots, members present at 2, 5 and 6 only. Gaps at the
# first slot and at an interior pair.
PRESENT_SLOTS = [2, 5, 6]
SLOT_COUNT = 6

ARTIFACT_DIR = os.path.join(
    PYTHON_ARTIFACTS,
    "database",
    "fileSeriesDocument",
    "testFileSeriesDocumentArtifacts",
)


def bytes_of(slot):
    """Member at *slot* holds ten bytes starting at slot*10.

    So a resolution that returns the wrong member is a failure rather than a
    pass, and the bytes are distinct from the manifest's own. The same rule
    is used on the MATLAB side, so each language can verify the other's
    bytes without sharing code.
    """
    return bytes(range(slot * 10, slot * 10 + 10))


class TestFileSeriesDocumentArtifacts:
    def test_file_series_document_artifacts(self):
        if os.path.isdir(ARTIFACT_DIR):
            shutil.rmtree(ARTIFACT_DIR)
        os.makedirs(ARTIFACT_DIR, exist_ok=True)

        db_path = os.path.join(ARTIFACT_DIR, DB_FILENAME)
        db = SQLiteDB(db_path)
        db.add_branch(BRANCH)
        db.set_branch(BRANCH)

        # Write the members where add_file_series will find them. They keep a
        # common parent so the manifest records a relative source name per
        # member rather than none.
        source_root = os.path.join(ARTIFACT_DIR, "members")
        os.makedirs(source_root, exist_ok=True)
        locations = []
        for slot in PRESENT_SLOTS:
            path = os.path.join(source_root, f"chunk_{slot}.bin")
            with open(path, "wb") as handle:
                handle.write(bytes_of(slot))
            locations.append(path)

        doc = Document("demoSeries", **{"demoSeries.value": 1})
        # delete_original=0 so the sources stay in the artifact for a reader
        # that wants to compare against them directly.
        doc.add_file_series(
            SERIES_NAME, locations, indices=PRESENT_SLOTS, delete_original=0
        )

        # Capture what the manifest recorded BEFORE add_docs, since
        # ingest_locations is stripped on the way in.
        member_uids = {
            entry["index"]: entry["uid"]
            for entry in doc.series_ingest_locations(SERIES_NAME)
        }
        manifest_uid = doc.file_uids(SERIES_NAME)[0]

        db.add_docs([doc], validate=False)

        absent_names = [
            f"{SERIES_NAME}_{slot}"
            for slot in range(1, SLOT_COUNT + 1)
            if slot not in PRESENT_SLOTS
        ]

        artifact = {
            "dbFilename": DB_FILENAME,
            "branchName": BRANCH,
            "docId": doc.id(),
            "seriesName": SERIES_NAME,
            "slotCount": SLOT_COUNT,
            "manifestUid": manifest_uid,
            # Where the bytes are, relative to the database file, so a reader
            # need not know how FileDir is derived to find them.
            "fileDirRelative": "files",
            "members": [
                {
                    "slot": slot,
                    "uid": member_uids[slot],
                    "filename": f"{SERIES_NAME}_{slot}",
                    "bytes": list(bytes_of(slot)),
                }
                for slot in PRESENT_SLOTS
            ],
            # These names parse as members and the manifest records no uid for
            # them: exist_doc must answer false and must NOT resolve them to a
            # neighbouring slot.
            "absentMemberNames": absent_names,
            # A files-table row per member is what this design refuses. The
            # reader asserts the count, not just the manifest's presence.
            "expectedFilesTableFilenames": [SERIES_NAME],
            # Present-but-empty, never absent. See commit 87ebcf6.
            "storedIngestLocationsAreEmptyNotAbsent": True,
        }
        with open(os.path.join(ARTIFACT_DIR, "manifest.json"), "w") as handle:
            json.dump(artifact, handle, indent=2)

        # --- self-check -------------------------------------------------
        # Everything claimed above, verified here before any cross-language
        # claim rests on it.

        # 1. every present member reads back byte for byte, by name.
        for slot in PRESENT_SLOTS:
            name = f"{SERIES_NAME}_{slot}"
            exists, _path = db.exist_doc(doc.id(), name)
            assert exists, f"Member {name} was not ingested."
            file_obj = db.open_doc(doc.id(), name)
            file_obj.fopen()
            try:
                assert file_obj.fread() == bytes_of(slot), f"Wrong bytes for {name}"
            finally:
                file_obj.fclose()

        # 2. the gaps answer false, and do not resolve to a neighbour.
        for name in absent_names:
            assert not db.exist_doc(doc.id(), name)[
                0
            ], f"An absent slot resolved: {name}"

        # 3. members land at <FileDir>/<uid>.
        for slot in PRESENT_SLOTS:
            assert os.path.isfile(
                os.path.join(db._file_dir(), member_uids[slot])
            ), f"member {slot} is not at FileDir/<uid>"

        # 4. one files-table row, the manifest's. THE design decision.
        cursor = db.dbid.cursor()
        rows = cursor.execute(
            "SELECT filename FROM docs, files "
            "WHERE docs.doc_id = ? AND files.doc_idx = docs.doc_idx",
            (doc.id(),),
        ).fetchall()
        assert [row["filename"] for row in rows] == [
            SERIES_NAME
        ], "a member must not get a files-table row of its own"

        # 5. the stored document keeps series_info with ingest_locations
        #    empty rather than removed.
        stored = db.get_docs(doc.id())
        stored_series = stored.document_properties["files"]["series_info"]
        if isinstance(stored_series, dict):
            stored_series = [stored_series]
        entry = next(
            e for e in stored_series if e["name"].lower() == SERIES_NAME.lower()
        )
        assert (
            "ingest_locations" in entry
        ), "ingest_locations must be present, not removed (87ebcf6)"
        assert (
            entry["ingest_locations"] == []
        ), "ingest_locations must be empty in a stored document"

        # 6. the manifest itself is still an ordinary file of the document,
        #    readable under the series' own name.
        file_obj = db.open_doc(doc.id(), SERIES_NAME)
        file_obj.fopen()
        try:
            assert (
                file_obj.fread(8) == b"DIDFSER1"
            ), "opening the series name must give the manifest itself"
        finally:
            file_obj.fclose()

        db._close_db()
