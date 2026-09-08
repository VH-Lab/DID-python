"""readArtifact symmetry test: open a database holding a file series the
other language ingested.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+readArtifacts/+database/fileSeriesDocument.m.

What this proves that no single-language test can: that the two
implementations agree about a series they did not both build. A series
member has no files-table row and no file_info entry of its own, so resolving
one is entirely a matter of reading the other language's manifest and
believing what it says about slots and uids. Get the slot base wrong, or the
uid offset, and a member resolves to its neighbour -- which is a pass to any
test that only checks that SOMETHING came back.

The database is copied out of the artifact directory before it is opened, so
this test cannot disturb an artifact the other language has yet to read. The
whole directory goes, not just the .sqlite: FileDir is ``files/`` beside the
database file, and that is where the member bytes live.

Skips when the artifact is absent rather than failing, so this can land in
either repository first without blocking the other. See conftest.py for the
CI-side gate that turns absence into a hard failure in the symmetry job.
"""

import json
import os
import shutil

from did.implementations.sqlitedb import SQLiteDB
from tests.symmetry.conftest import SYMMETRY_BASE, missing_artifact


class TestReadFileSeriesDocument:
    def test_file_series_document_artifacts(self, source_type, tmp_path):
        artifact_dir = os.path.join(
            SYMMETRY_BASE,
            source_type,
            "database",
            "fileSeriesDocument",
            "testFileSeriesDocumentArtifacts",
        )
        if not os.path.isdir(artifact_dir):
            missing_artifact(
                f"Artifact directory from {source_type} does not exist: {artifact_dir}"
            )
        manifest_file = os.path.join(artifact_dir, "manifest.json")
        if not os.path.isfile(manifest_file):
            missing_artifact(
                f"manifest.json not found in {source_type} artifact directory."
            )

        with open(manifest_file, "r") as handle:
            manifest = json.load(handle)

        # Work on a copy, and take the whole directory: the member bytes live
        # in FileDir beside the database, not inside it.
        scratch = str(tmp_path / "fileSeriesDocument")
        shutil.copytree(artifact_dir, scratch)

        db = SQLiteDB(os.path.join(scratch, manifest["dbFilename"]))
        try:
            doc_id = manifest["docId"]
            series_name = manifest["seriesName"]
            doc = db.get_docs(doc_id)

            # --- the document still knows its series ---------------------
            assert doc.is_file_series(
                series_name
            ), f"the document from {source_type} lost its series declaration"
            _count, n_present = doc.series_count(series_name)
            assert n_present == len(manifest["members"])

            # --- 1. every present member reads back byte for byte --------
            # By NAME, so the whole NAME_<i> -> manifest slot -> uid ->
            # bytes chain is exercised, which is the chain that has a
            # one-based/zero-based boundary in the middle of it.
            for member in manifest["members"]:
                name = member["filename"]
                expected = bytes(member["bytes"])

                exists, path = db.exist_doc(doc_id, name)
                assert exists, f"{source_type}: member {name} did not resolve"
                assert os.path.basename(path) == member["uid"], (
                    f"{name} resolved to a file not named by the manifest's uid "
                    f"for that slot"
                )

                file_obj = db.open_doc(doc_id, name)
                file_obj.fopen()
                try:
                    assert file_obj.fread() == expected, (
                        f"{source_type}: wrong bytes for {name} -- a slot or "
                        f"uid offset that is off by one lands on a neighbour"
                    )
                finally:
                    file_obj.fclose()

            # --- 2. the gaps answer false, and never a neighbour ---------
            for name in manifest["absentMemberNames"]:
                assert not db.exist_doc(doc_id, name)[
                    0
                ], f"{source_type}: absent slot {name} resolved to something"

            # --- 3. members are at FileDir/<uid> -------------------------
            file_dir = os.path.join(scratch, manifest["fileDirRelative"])
            for member in manifest["members"]:
                assert os.path.isfile(os.path.join(file_dir, member["uid"])), (
                    f"{source_type}: member {member['slot']} is not at "
                    f"FileDir/<uid>"
                )

            # --- 4. one files-table row, the manifest's ------------------
            # THE design decision: 28,000 members must not become 28,000
            # rows. A port that "fixed" member reads by giving each one a row
            # would pass every other check here.
            cursor = db.dbid.cursor()
            rows = cursor.execute(
                "SELECT filename FROM docs, files "
                "WHERE docs.doc_id = ? AND files.doc_idx = docs.doc_idx",
                (doc_id,),
            ).fetchall()
            assert sorted(row["filename"] for row in rows) == sorted(
                manifest["expectedFilesTableFilenames"]
            ), (
                f"{source_type}: a series member must not get a files-table "
                f"row of its own"
            )

            # --- 5. ingest_locations present but empty, never removed ----
            if manifest.get("storedIngestLocationsAreEmptyNotAbsent"):
                series_info = doc.document_properties["files"]["series_info"]
                if isinstance(series_info, dict):
                    series_info = [series_info]
                entry = next(
                    e
                    for e in series_info
                    if str(e.get("name", "")).lower() == series_name.lower()
                )
                assert "ingest_locations" in entry, (
                    f"{source_type}: ingest_locations was REMOVED from the "
                    f"stored document rather than emptied. A document read "
                    f"back then has a narrower shape than a fresh one, and "
                    f"adding a series to it fails much later (87ebcf6)"
                )
                assert not entry["ingest_locations"], (
                    f"{source_type}: a stored document must not carry member "
                    f"source paths"
                )

            # --- 6. the manifest is still an ordinary file ---------------
            file_obj = db.open_doc(doc_id, series_name)
            file_obj.fopen()
            try:
                assert file_obj.fread(8) == b"DIDFSER1", (
                    f"{source_type}: opening the series name must give the "
                    f"manifest itself"
                )
            finally:
                file_obj.fclose()

            # --- 7. the accessors agree with the manifest ----------------
            indices, uids = db.series_members(doc, series_name)
            assert indices == [m["slot"] for m in manifest["members"]]
            assert uids == [m["uid"] for m in manifest["members"]]
            for member in manifest["members"]:
                assert db.series_has(doc, series_name, member["slot"])
            for slot in range(1, manifest["slotCount"] + 1):
                if slot not in indices:
                    assert not db.series_has(doc, series_name, slot), (
                        f"{source_type}: series_has claims a member at empty "
                        f"slot {slot}"
                    )
        finally:
            db._close_db()
