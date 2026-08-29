"""makeArtifact symmetry test: record where this language puts the file cache.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+makeArtifacts/+common/pathAgreement.m.

The two languages share one file cache directory. Everything else in the
symmetry suite checks that they agree on the *contents* of that directory;
nothing checked that they agree on *which* directory, and that is the
assumption all the rest rests on. If one side's path changes, the caches
silently stop being shared -- no error, no failing test, just two
half-populated caches and every file fetched twice.

So each language writes its filecachepath here and the other asserts it
matches its own. Both run in the same job with the same environment, which
is what makes the comparison meaningful.

Artifacts are written to:
    <tempdir>/DID/symmetryTest/pythonArtifacts/common/pathAgreement/testPathAgreementArtifacts/
"""

import json
import os
import shutil
from pathlib import Path

from tests.symmetry.conftest import PYTHON_ARTIFACTS

ARTIFACT_DIR = os.path.join(
    PYTHON_ARTIFACTS, "common", "pathAgreement", "testPathAgreementArtifacts"
)


class TestPathAgreementArtifacts:
    def test_path_agreement_artifacts(self, real_file_cache_path):
        # real_file_cache_path, not PathConstants().filecachepath: the
        # autouse fixture in tests/conftest.py redirects the cache to a temp
        # directory for every test, and recording that would compare two
        # redirects rather than the two languages.
        if os.path.isdir(ARTIFACT_DIR):
            shutil.rmtree(ARTIFACT_DIR)
        os.makedirs(ARTIFACT_DIR)

        home = str(Path.home())
        manifest = {
            "fileCachePath": real_file_cache_path,
            "homeDirectory": home,
        }
        with open(os.path.join(ARTIFACT_DIR, "manifest.json"), "w") as handle:
            json.dump(manifest, handle, indent=2)

        # Self-check before asking the other language to agree with it.
        assert os.path.normpath(real_file_cache_path) == os.path.normpath(
            os.path.join(home, "Documents", "DID", "fileCache")
        )
