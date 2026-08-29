"""readArtifact symmetry test: open a file-bearing document's files.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+readArtifacts/+database/fileDocument.m.

Parameterized over SOURCE_TYPES, so it reads artifacts produced by either the
Python makeArtifact test or the MATLAB one. Reading the MATLAB artifact is the
half that matters most: MATLAB ingests a local file into its FileDir and then
deletes the original, so the document JSON's location no longer exists on disk
and only the `files` table can find the file.

Skips when the artifact is absent rather than failing, so this test can land in
either repository first without blocking the other -- each repository's
symmetry job checks out the other's main branch.
"""

import json
import os

import pytest

from did.implementations.sqlitedb import SQLiteDB
from tests.symmetry.conftest import SYMMETRY_BASE


class TestReadFileDocument:
    """Read a file-bearing document's files from either language's artifacts."""

    def test_file_document_artifacts(self, source_type):
        artifact_dir = os.path.join(
            SYMMETRY_BASE,
            source_type,
            "database",
            "fileDocument",
            "testFileDocumentArtifacts",
        )
        if not os.path.isdir(artifact_dir):
            pytest.skip(
                f"Artifact directory from {source_type} does not exist: {artifact_dir}"
            )

        manifest_file = os.path.join(artifact_dir, "manifest.json")
        if not os.path.isfile(manifest_file):
            pytest.skip(f"manifest.json not found in {source_type} artifact directory.")

        with open(manifest_file, "r") as handle:
            manifest = json.load(handle)

        db_path = os.path.join(artifact_dir, manifest["dbFilename"])
        if not os.path.isfile(db_path):
            pytest.skip(f"Database file not found: {db_path}")

        db = SQLiteDB(db_path)
        try:
            doc_id = manifest["docId"]

            # The document itself must be there.
            doc = db.get_docs(doc_id, OnMissing="ignore")
            assert (
                doc is not None
            ), f"Document {doc_id} from {source_type} not found in the database"

            # Every file must open and hold exactly the bytes the maker wrote.
            for entry in manifest["files"]:
                name = entry["name"]
                expected = bytes(entry["bytes"])

                file_obj = db.open_doc(doc_id, name)
                file_obj.fopen()
                assert file_obj.fid is not None, (
                    f"Could not open {name} from {source_type}. A MATLAB-written "
                    f"document has had its original deleted after ingestion, so "
                    f"this resolves only through the files table."
                )
                try:
                    actual = file_obj.fread()
                finally:
                    file_obj.fclose()

                assert actual == expected, (
                    f"Content mismatch for {name} from {source_type}: "
                    f"expected {len(expected)} bytes {list(expected)}, "
                    f"got {len(actual)} bytes {list(actual)}"
                )
        finally:
            db._close_db()
