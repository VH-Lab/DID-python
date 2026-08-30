"""readArtifact symmetry test: read and validate DID database artifacts.

Mirrors DID-matlab's tests/+did/+symmetry/+readArtifacts/+database/buildDatabase.m.

This test is parameterized over SOURCE_TYPES (matlabArtifacts, pythonArtifacts).
It reads artifacts produced by either the Python makeArtifact test above or by
the MATLAB makeArtifact test, then validates them against a live database summary.

It also runs Python's own schema validator over whatever the other language
wrote (DID-python#28). Round-tripping a database proves each language can read
the other's bytes; it does not prove either considers the other's documents
schema-valid, because validation lives in add_docs and each language only ever
calls add_docs on documents it created itself.
"""

import json
import os

from did.implementations.sqlitedb import SQLiteDB
from did.util import compare_database_summary, database_summary
from did.validate import ValidationError
from tests.symmetry.conftest import SYMMETRY_BASE, missing_artifact


class TestReadBuildDatabase:
    """Read and validate DID database artifacts for cross-language symmetry testing."""

    def test_build_database_artifacts(self, source_type):
        artifact_dir = os.path.join(
            SYMMETRY_BASE,
            source_type,
            "database",
            "buildDatabase",
            "testBuildDatabaseArtifacts",
        )

        if not os.path.isdir(artifact_dir):
            missing_artifact(
                f"Artifact directory from {source_type} does not exist: {artifact_dir}"
            )

        # Step 1: Load the saved summary
        summary_file = os.path.join(artifact_dir, "summary.json")
        if not os.path.isfile(summary_file):
            missing_artifact(
                f"summary.json not found in {source_type} artifact directory."
            )

        with open(summary_file, "r") as f:
            saved_summary = json.load(f)

        assert "branchNames" in saved_summary, "summary.json missing branchNames field."
        assert "dbFilename" in saved_summary, "summary.json missing dbFilename field."

        # Step 2: Open the DID database and produce a live summary
        db_path = os.path.join(artifact_dir, saved_summary["dbFilename"])
        if not os.path.isfile(db_path):
            missing_artifact(f"Database file not found: {db_path}")

        db = SQLiteDB(db_path)
        live_summary = database_summary(db)

        # Step 3: Compare the saved summary against the live database summary
        report = compare_database_summary(saved_summary, live_summary)
        assert (
            report == []
        ), f"Database summary mismatch for {source_type}: {'; '.join(report)}"

        # Step 4: Also verify per-branch JSON files match the live database
        branch_names = saved_summary["branchNames"]
        if isinstance(branch_names, str):
            branch_names = [branch_names]

        json_branches_dir = os.path.join(artifact_dir, "jsonBranches")
        if not os.path.isdir(json_branches_dir):
            missing_artifact(f"jsonBranches directory not found in {source_type}")

        for branch_name in branch_names:
            branch_json_file = os.path.join(
                json_branches_dir, f"branch_{branch_name}.json"
            )
            if not os.path.isfile(branch_json_file):
                missing_artifact(
                    f"Branch JSON file missing for {branch_name} in {source_type}"
                )

            with open(branch_json_file, "r") as f:
                saved_branch = json.load(f)

            # Verify document count matches the live database
            actual_doc_ids = db.get_doc_ids(branch_name)
            assert (
                len(actual_doc_ids) == saved_branch["docCount"]
            ), f"Document count mismatch in branch {branch_name} from {source_type}"

            # Verify each saved document exists in the live database
            saved_docs = saved_branch.get("documents", [])
            live_docs = []
            for saved_doc in saved_docs:
                expected_id = saved_doc["id"]
                doc = db.get_docs(expected_id, OnMissing="ignore")
                assert doc is not None, (
                    f"Document {expected_id} from {source_type} "
                    f"not found in database branch {branch_name}"
                )

                if doc is not None:
                    live_docs.append(doc)
                    actual_props = doc.document_properties
                    actual_class = actual_props.get("document_class", {}).get(
                        "class_name", ""
                    )
                    assert actual_class == saved_doc["className"], (
                        f"Class name mismatch for doc {expected_id} "
                        f"in branch {branch_name} from {source_type}"
                    )

            # Step 5: run OUR schema validator over documents the other
            # language wrote.
            #
            # Without this the suite only ever proved each language can READ
            # the other's database. Validation lives in add_docs, and each
            # language calls add_docs only on documents it created itself
            # during makeArtifacts -- so a document one language writes could
            # be schema-invalid under the other's validator with the suite
            # staying green. That is not hypothetical: every Python document
            # id was a UUID4 where base.schema.json types base.id as did_uid,
            # so MATLAB's add_docs would have rejected all of them, and this
            # suite noticed nothing until someone read the code. See
            # DID-python#28 / DID-matlab#155.
            try:
                db.validate_docs(live_docs)
            except ValidationError as error:
                raise AssertionError(
                    f"Documents written by {source_type} do not pass this "
                    f"language's schema validator, in branch {branch_name}: "
                    f"[{error.identifier}] {error}"
                ) from error

        db._close_db()
