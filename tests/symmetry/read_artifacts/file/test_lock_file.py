"""readArtifact symmetry test: honour lock files the other language wrote.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+readArtifacts/+file/lockFile.m.

The maker leaves one live lock and one already expired. This side must
refuse the first and reclaim the second. Reclaiming is the half that was
broken: DID-matlab wrote its expiry as char(datetime(...)) -- "29-Aug-2026
14:35:12" -- which datetime.fromisoformat cannot parse, and
checkout_lock_file swallowed the ValueError, so the lock read as one that
never expires. A MATLAB process that died holding it locked Python out of a
shared cache permanently.

The locks are copied out of the artifact directory first, since reclaiming
one deletes it and the other language may not have read it yet.
"""

import json
import os
import shutil

from did.file import checkout_lock_file, parse_lock_expiration, release_lock_file
from tests.symmetry.conftest import SYMMETRY_BASE, missing_artifact


class TestReadLockFile:
    def _artifacts(self, source_type, tmp_path):
        artifact_dir = os.path.join(
            SYMMETRY_BASE, source_type, "file", "lockFile", "testLockFileArtifacts"
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
        with open(manifest_file) as handle:
            manifest = json.load(handle)

        working = str(tmp_path / "locks")
        os.makedirs(working)
        for key in ("liveLock", "expiredLock"):
            shutil.copyfile(
                os.path.join(artifact_dir, manifest[key]),
                os.path.join(working, manifest[key]),
            )
        return manifest, working

    def test_both_expiries_are_readable(self, source_type, tmp_path):
        manifest, working = self._artifacts(source_type, tmp_path)
        for key in ("liveLock", "expiredLock"):
            path = os.path.join(working, manifest[key])
            with open(path) as handle:
                first = handle.readline()
            # Must not raise: an unparseable expiry is indistinguishable from
            # a lock that never expires.
            parse_lock_expiration(first)

    def test_an_expired_lock_is_reclaimed(self, source_type, tmp_path):
        manifest, working = self._artifacts(source_type, tmp_path)
        path = os.path.join(working, manifest["expiredLock"])

        _, key = checkout_lock_file(path, check_loops=3, throw_error=False)
        assert key, (
            f"could not reclaim an expired lock written by {source_type}. A "
            f"crashed process would wedge a shared cache permanently."
        )
        release_lock_file(path, key)

    def test_a_live_lock_is_not_stolen(self, source_type, tmp_path):
        manifest, working = self._artifacts(source_type, tmp_path)
        path = os.path.join(working, manifest["liveLock"])

        _, key = checkout_lock_file(path, check_loops=1, throw_error=False)
        assert key is None, f"took a live lock held by {source_type}"
        assert os.path.isfile(path), "and deleted it"
