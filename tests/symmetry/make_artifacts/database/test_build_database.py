"""makeArtifact symmetry test: build a DID database and export summary artifacts.

Mirrors DID-matlab's tests/+did/+symmetry/+makeArtifacts/+database/buildDatabase.m.

Artifacts are written to:
    <tempdir>/DID/symmetryTest/pythonArtifacts/database/buildDatabase/testBuildDatabaseArtifacts/

The readArtifact counterpart (and the MATLAB readArtifact test) will later open
these artifacts and compare them against a live database summary.
"""

import json
import os
import random
import shutil

import numpy as np

from did.implementations.sqlitedb import SQLiteDB
from did.util import compare_database_summary, database_summary
from tests.helpers import make_doc_tree
from tests.symmetry.conftest import PYTHON_ARTIFACTS

DB_FILENAME = "symmetry_test.sqlite"

ARTIFACT_DIR = os.path.join(
    PYTHON_ARTIFACTS,
    "database",
    "buildDatabase",
    "testBuildDatabaseArtifacts",
)


class TestBuildDatabase:
    """Generate DID database artifacts for cross-language symmetry testing."""

    def test_build_database_artifacts(self):
        # Use fixed seeds for reproducibility
        random.seed(0)
        np.random.seed(0)

        # Clean previous artifacts
        if os.path.isdir(ARTIFACT_DIR):
            shutil.rmtree(ARTIFACT_DIR)
        os.makedirs(ARTIFACT_DIR, exist_ok=True)

        # Step 1: Create the database
        db_path = os.path.join(ARTIFACT_DIR, DB_FILENAME)
        db = SQLiteDB(db_path)

        # Step 2: Create 3 branches in a simple hierarchy:
        #   branch_main
        #     +-- branch_dev
        #     +-- branch_feature
        branch_names = ["branch_main", "branch_dev", "branch_feature"]

        # Root branch with documents
        db.add_branch(branch_names[0])
        _, _, root_docs = make_doc_tree([3, 3, 3])
        db.add_docs(root_docs, branch_id=branch_names[0])

        # branch_dev as child of branch_main
        db.set_branch(branch_names[0])
        db.add_branch(branch_names[1])
        _, _, dev_docs = make_doc_tree([2, 2, 2])
        db.add_docs(dev_docs, branch_id=branch_names[1])

        # branch_feature as child of branch_main
        db.set_branch(branch_names[0])
        db.add_branch(branch_names[2])
        _, _, feature_docs = make_doc_tree([2, 1, 2])
        db.add_docs(feature_docs, branch_id=branch_names[2])

        # Step 3: Generate summary
        summary = database_summary(db)
        summary["dbFilename"] = DB_FILENAME

        # Step 4: Write per-branch JSON files
        json_branches_dir = os.path.join(ARTIFACT_DIR, "jsonBranches")
        os.makedirs(json_branches_dir, exist_ok=True)

        for branch_name in branch_names:
            branch_data = summary["branches"][branch_name]
            branch_json_path = os.path.join(
                json_branches_dir, f"branch_{branch_name}.json"
            )
            with open(branch_json_path, "w") as f:
                json.dump(branch_data, f, indent=2)

        # Write the full summary JSON
        summary_path = os.path.join(ARTIFACT_DIR, "summary.json")
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        # Step 5: Verify artifacts were created
        assert os.path.isfile(db_path), "Database file was not created."
        assert os.path.isfile(summary_path), "summary.json was not created."
        for branch_name in branch_names:
            branch_file = os.path.join(json_branches_dir, f"branch_{branch_name}.json")
            assert os.path.isfile(
                branch_file
            ), f"Branch JSON file missing for {branch_name}"

        # Step 6: Self-check -- re-summarize and compare
        summary_check = database_summary(db)
        summary_check["dbFilename"] = DB_FILENAME
        self_report = compare_database_summary(summary, summary_check)
        assert self_report == [], f"Self-check failed: {'; '.join(self_report)}"

        db._close_db()
