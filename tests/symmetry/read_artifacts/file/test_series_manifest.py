"""readArtifact symmetry test: open a series manifest the other language wrote.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+readArtifacts/+file/seriesManifest.m (once
that exists; today the make-side lives on both sides and this consumes
whichever writer produced the artifact).

Parameterized over ``SOURCE_TYPES``, so it reads a manifest built by either
language. The MATLAB direction is the important one -- it proves the two
readers agree on the shared binary format, byte for byte.

Skips when the artifact is absent rather than failing, so this can land in
either repository first without blocking the other. See conftest.py for the
CI-side gate that turns absence into a hard failure in the symmetry job.
"""

import json
import os
import shutil

from did.file import read_series_manifest, read_series_manifest_uid
from tests.symmetry.conftest import SYMMETRY_BASE, missing_artifact


class TestReadSeriesManifest:
    def test_series_manifest_artifacts(self, source_type, tmp_path):
        artifact_dir = os.path.join(
            SYMMETRY_BASE,
            source_type,
            "file",
            "seriesManifest",
            "testSeriesManifestArtifacts",
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

        # Copy artifacts to a scratch directory so this test does not disturb
        # anything the other language has yet to read.
        scratch = tmp_path / "seriesManifest"
        shutil.copytree(artifact_dir, scratch)

        with open(manifest_file, "r") as f:
            manifest_data = json.load(f)

        assert manifest_data["magic"] == "DIDFSER1"
        assert manifest_data["formatVersion"] == 1
        assert manifest_data["indexBase"] == "zero"

        for entry in manifest_data["files"]:
            path = scratch / entry["file"]
            assert path.is_file(), entry["file"]

            m = read_series_manifest(str(path))
            assert m["count"] == entry["count"], entry["name"]
            assert m["uid_width"] == entry["uidWidth"], entry["name"]
            assert m["uids"] == entry["uids"], entry["name"]
            assert m["has_source_names"] == entry["hasSourceNames"], entry["name"]
            if entry["hasSourceNames"]:
                assert m["source_names"] == entry["sourceNames"], entry["name"]
            else:
                assert m["source_names"] == [], entry["name"]

            # O(1) slot read agrees with the whole read for every slot,
            # including the absent-member slots.
            for slot in range(1, entry["count"] + 1):
                uid, count = read_series_manifest_uid(str(path), slot)
                assert uid == entry["uids"][slot - 1], f"{entry['name']} slot {slot}"
                assert count == entry["count"], entry["name"]

            # One past the end is a miss, not an error.
            uid, _count = read_series_manifest_uid(str(path), entry["count"] + 1)
            assert uid == "", entry["name"]
