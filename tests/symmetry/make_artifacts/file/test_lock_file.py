"""makeArtifact symmetry test: leave lock files for the other language to read.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+makeArtifacts/+file/lockFile.m.

Once both languages share a file cache they contend for the same
"<file>-lock". Agreeing on the name is not enough: a reader that cannot
parse the other's expiry cannot tell an expired lock from an unreadable
one, so a process that died holding the lock would shut the other language
out permanently -- the exact crash the one-hour expiry exists to survive.

Two locks are left behind: one live, one already expired. The other
language must refuse the first and reclaim the second. Nothing here needs
real concurrency, which keeps it deterministic.

Artifacts are written to:
    <tempdir>/DID/symmetryTest/pythonArtifacts/file/lockFile/testLockFileArtifacts/
"""

import datetime as dt
import json
import os
import shutil

from tests.symmetry.conftest import PYTHON_ARTIFACTS

ARTIFACT_DIR = os.path.join(
    PYTHON_ARTIFACTS, "file", "lockFile", "testLockFileArtifacts"
)

LIVE_LOCK = "live.bin-lock"
EXPIRED_LOCK = "expired.bin-lock"
LIVE_KEY = "symmetryLiveKey"
EXPIRED_KEY = "symmetryExpiredKey"


def write_lock(path, when, key):
    """Write a lock file exactly as did.file.checkout_lock_file does."""
    with open(path, "w") as handle:
        handle.write(f"{when.isoformat()}\n{key}")


class TestLockFileArtifacts:
    def test_lock_file_artifacts(self):
        if os.path.isdir(ARTIFACT_DIR):
            shutil.rmtree(ARTIFACT_DIR)
        os.makedirs(ARTIFACT_DIR)

        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        live = now + dt.timedelta(hours=1)
        expired = now - dt.timedelta(hours=1)

        write_lock(os.path.join(ARTIFACT_DIR, LIVE_LOCK), live, LIVE_KEY)
        write_lock(os.path.join(ARTIFACT_DIR, EXPIRED_LOCK), expired, EXPIRED_KEY)

        manifest = {
            "liveLock": LIVE_LOCK,
            "expiredLock": EXPIRED_LOCK,
            "liveKey": LIVE_KEY,
            "expiredKey": EXPIRED_KEY,
        }
        with open(os.path.join(ARTIFACT_DIR, "manifest.json"), "w") as handle:
            json.dump(manifest, handle, indent=2)

        # Self-check: our own reader must agree about both before we ask the
        # other language to.
        from did.file import parse_lock_expiration

        for name in (LIVE_LOCK, EXPIRED_LOCK):
            with open(os.path.join(ARTIFACT_DIR, name)) as handle:
                parse_lock_expiration(handle.readline())
