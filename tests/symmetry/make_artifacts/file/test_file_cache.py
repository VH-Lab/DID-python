"""makeArtifact symmetry test: build a file cache both languages can read.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+makeArtifacts/+file/fileCache.m.

Both languages call the cache index ".fileCacheInfo" and, since the port,
both write MATLAB's binary layout: a 26-byte header (fileNameCharacters as
uint16, then maxSize, reduceSize and currentSize as uint64) followed by
fixed-width {char[n], double, uint64} rows. Unit tests in each language
assert that layout from the inside. This pair is the only thing that checks
one language can read what the other actually wrote.

It matters because the two are expected to share a directory: MATLAB's
do_open_doc consults filecachepath on every document-file open, and
DID-python's open_doc now does the same.

Artifacts are written to:
    <tempdir>/DID/symmetryTest/pythonArtifacts/file/fileCache/testFileCacheArtifacts/
"""

import json
import os
import shutil

from did.file import BinaryTable, FileCache, Fileobj
from tests.symmetry.conftest import PYTHON_ARTIFACTS

NAME_CHARACTERS = 33
MAX_SIZE = 100000
REDUCE_SIZE = 80000

# Fixed last-access times rather than "now", so both languages can compare
# the stored doubles exactly. A format error shows up as a wildly different
# number, not as a rounding difference.
FIXED_TIMES = [738000.5, 738001.25, 738002.75]
SIZES = [10, 20, 30]

ARTIFACT_DIR = os.path.join(
    PYTHON_ARTIFACTS, "file", "fileCache", "testFileCacheArtifacts"
)
CACHE_DIR_NAME = "cache"


def name_of(index):
    """The 33-character name a cached file takes: a did unique id is 33 long."""
    return f"{index:033d}"


def expected_bytes(index, size):
    """Deterministic content: file *index* is `size` copies of byte `index`."""
    return bytes([index]) * size


class TestFileCacheArtifacts:
    def test_file_cache_artifacts(self):
        if os.path.isdir(ARTIFACT_DIR):
            shutil.rmtree(ARTIFACT_DIR)
        cache_dir = os.path.join(ARTIFACT_DIR, CACHE_DIR_NAME)
        source_dir = os.path.join(ARTIFACT_DIR, "sources")
        os.makedirs(cache_dir)
        os.makedirs(source_dir)

        cache = FileCache(cache_dir, NAME_CHARACTERS, MAX_SIZE, REDUCE_SIZE)
        for index, size in enumerate(SIZES, start=1):
            source = os.path.join(source_dir, f"source_{index}")
            with open(source, "wb") as handle:
                handle.write(expected_bytes(index, size))
            cache.add_file(source, name_of(index))

        # Stamp known access times over the "now" values add_file wrote, so
        # the reader can assert the exact doubles rather than a tolerance.
        table = BinaryTable(
            Fileobj(
                fullpathfilename=os.path.join(cache_dir, FileCache.CACHE_INFO_FILE_NAME)
            ),
            ["char", "double", "uint64"],
            [NAME_CHARACTERS, 8, 8],
            [NAME_CHARACTERS, 1, 1],
            FileCache.HEADER_SIZE,
        )
        for row, when in enumerate(FIXED_TIMES, start=1):
            table.write_entry(row, 2, when)

        manifest = {
            "cacheDirName": CACHE_DIR_NAME,
            "fileNameCharacters": NAME_CHARACTERS,
            "maxSize": MAX_SIZE,
            "reduceSize": REDUCE_SIZE,
            "currentSize": sum(SIZES),
            "entries": [
                {
                    "name": name_of(index),
                    "size": size,
                    "lastAccess": FIXED_TIMES[index - 1],
                    "bytes": list(expected_bytes(index, size)),
                }
                for index, size in enumerate(SIZES, start=1)
            ],
        }
        with open(os.path.join(ARTIFACT_DIR, "manifest.json"), "w") as handle:
            json.dump(manifest, handle, indent=2)

        # Self-check: reopen and confirm what we claim, before any
        # cross-language claim is made about it.
        reopened = FileCache(cache_dir)
        assert reopened.file_name_characters == NAME_CHARACTERS
        assert reopened.max_size == MAX_SIZE
        assert reopened.get_properties()["currentSize"] == sum(SIZES)
        names, sizes, last_access = reopened.file_list()
        assert names == [name_of(i) for i in (1, 2, 3)]
        assert sizes == SIZES
        assert last_access == FIXED_TIMES
