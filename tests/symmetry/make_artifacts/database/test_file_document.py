"""makeArtifact symmetry test: build a database holding a file-bearing document.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+makeArtifacts/+database/fileDocument.m.

The buildDatabase symmetry pair covers demoA/demoB/demoC documents, none of
which declares a ``files`` section, and it compares database summaries -- which
carry no file information. So before this test nothing exercised the file
subsystem across languages at all: no document in the suite had a file, nothing
called open_doc, and the compared summary had nowhere to show a difference.

That is the gap that let a real break survive: DID-python created the ``files``
table but never inserted into it, and MATLAB's do_open_doc resolves a file only
through that table, so every file in a Python-written document was unreachable
from MATLAB.

Artifacts are written to:
    <tempdir>/DID/symmetryTest/pythonArtifacts/database/fileDocument/testFileDocumentArtifacts/
"""

import json
import os
import shutil

from did.document import Document
from did.implementations.sqlitedb import SQLiteDB
from tests.symmetry.conftest import PYTHON_ARTIFACTS

DB_FILENAME = "file_document_test.sqlite"
BRANCH = "branch_main"

# demoFile declares exactly these two files, both mustbenotempty.
FILE_NAMES = ["filename1.ext", "filename2.ext"]

ARTIFACT_DIR = os.path.join(
    PYTHON_ARTIFACTS,
    "database",
    "fileDocument",
    "testFileDocumentArtifacts",
)


def expected_bytes(index):
    """Deterministic content for file *index* (0-based): 10 consecutive bytes.

    File 0 holds 0..9, file 1 holds 10..19. The same rule is used on the MATLAB
    side, so each language can verify the other's bytes without sharing code.
    """
    return bytes(range(index * 10, index * 10 + 10))


class TestFileDocumentArtifacts:
    """Generate a database with a file-bearing document, for symmetry testing."""

    def test_file_document_artifacts(self):
        if os.path.isdir(ARTIFACT_DIR):
            shutil.rmtree(ARTIFACT_DIR)
        os.makedirs(ARTIFACT_DIR, exist_ok=True)

        db_path = os.path.join(ARTIFACT_DIR, DB_FILENAME)
        db = SQLiteDB(db_path)
        db.add_branch(BRANCH)
        db.set_branch(BRANCH)

        # Write the source files inside the artifact directory so the artifact
        # is self-contained. DID-python does no ingestion, so these originals
        # are what its own open_doc resolves; MATLAB reading this artifact
        # retrieves them through the files table's orig_location.
        doc = Document("demoFile", **{"demoFile.value": 1})
        for index, name in enumerate(FILE_NAMES):
            source = os.path.join(ARTIFACT_DIR, f"source_{name}")
            with open(source, "wb") as handle:
                handle.write(expected_bytes(index))
            doc.add_file(name, source)

        db.add_docs([doc])

        # A manifest the reader can check against without recomputing anything.
        manifest = {
            "dbFilename": DB_FILENAME,
            "branchName": BRANCH,
            "docId": doc.id(),
            "files": [
                {"name": name, "bytes": list(expected_bytes(index))}
                for index, name in enumerate(FILE_NAMES)
            ],
        }
        with open(os.path.join(ARTIFACT_DIR, "manifest.json"), "w") as handle:
            json.dump(manifest, handle, indent=2)

        # Self-check: the document must be readable through open_doc here,
        # before any cross-language claim is made about it.
        for index, name in enumerate(FILE_NAMES):
            file_obj = db.open_doc(doc.id(), name)
            file_obj.fopen()
            assert file_obj.fid is not None, f"could not open {name}"
            assert file_obj.fread() == expected_bytes(index), f"wrong bytes for {name}"
            file_obj.fclose()

        # And the files table must actually have a row per file -- the thing
        # MATLAB reads, and the thing that was empty before this was fixed.
        cursor = db.dbid.cursor()
        rows = cursor.execute(
            "SELECT f.filename FROM docs d, files f "
            "WHERE d.doc_idx = f.doc_idx AND d.doc_id = ?",
            (doc.id(),),
        ).fetchall()
        assert sorted(r["filename"] for r in rows) == sorted(FILE_NAMES), (
            "the files table must carry a row per file; MATLAB's open_doc "
            "reads nothing else"
        )

        db._close_db()
