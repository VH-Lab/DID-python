"""readArtifact symmetry test: check the other language names the same cache.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+readArtifacts/+common/pathAgreement.m.

This is the assumption every other file-cache symmetry test rests on. They
check that the two languages agree on the contents of the shared directory;
this checks they agree on which directory. A divergence here has no
symptom -- no error, no failing test, just two half-populated caches and
every file fetched twice.
"""

import json
import os

import pytest

from tests.symmetry.conftest import SYMMETRY_BASE


class TestReadPathAgreement:
    def test_path_agreement_artifacts(self, source_type, real_file_cache_path):
        artifact_dir = os.path.join(
            SYMMETRY_BASE,
            source_type,
            "common",
            "pathAgreement",
            "testPathAgreementArtifacts",
        )
        if not os.path.isdir(artifact_dir):
            pytest.skip(
                f"Artifact directory from {source_type} does not exist: {artifact_dir}"
            )
        manifest_file = os.path.join(artifact_dir, "manifest.json")
        if not os.path.isfile(manifest_file):
            pytest.skip(f"manifest.json not found in {source_type} artifact directory.")

        with open(manifest_file) as handle:
            manifest = json.load(handle)

        # normpath on both sides: MATLAB's fullfile and os.path.join can
        # differ over a trailing separator without disagreeing about the
        # directory, and that is not what this test is about.
        assert os.path.normpath(manifest["fileCachePath"]) == os.path.normpath(
            real_file_cache_path
        ), (
            f"{source_type} names a different file cache directory. The two "
            f"languages share this cache, so a divergence here means each "
            f"silently keeps its own and every file is fetched twice."
        )
