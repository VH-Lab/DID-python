"""readArtifact symmetry test: open a file cache the other language wrote.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+readArtifacts/+file/fileCache.m.

Parameterized over SOURCE_TYPES, so it reads a cache built by either
language. The MATLAB direction is the one that could not have worked before
the port: DID-python used to write this index as JSON under the same
".fileCacheInfo" name, so neither language could read the other's.

The cache is copied out of the artifact directory before it is opened, so
this test cannot disturb an artifact the other language has yet to read.

Skips when the artifact is absent rather than failing, so this can land in
either repository first without blocking the other.
"""

import json
import os
import shutil

from did.file import FileCache
from tests.symmetry.conftest import SYMMETRY_BASE, missing_artifact


class TestReadFileCache:
    def test_file_cache_artifacts(self, source_type, tmp_path):
        artifact_dir = os.path.join(
            SYMMETRY_BASE, source_type, "file", "fileCache", "testFileCacheArtifacts"
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

        # Work on a copy: the other language may not have read this yet.
        cache_dir = str(tmp_path / "cache")
        shutil.copytree(os.path.join(artifact_dir, manifest["cacheDirName"]), cache_dir)

        cache = FileCache(cache_dir)

        assert cache.file_name_characters == manifest["fileNameCharacters"], (
            f"the name width in {source_type}'s header was misread; every row "
            f"is fixed-width, so the whole index would be read at the wrong offsets"
        )
        properties = cache.get_properties()
        assert properties["maxSize"] == manifest["maxSize"]
        assert properties["reduceSize"] == manifest["reduceSize"]
        assert properties["currentSize"] == manifest["currentSize"]

        names, sizes, last_access = cache.file_list()
        expected = manifest["entries"]
        assert names == [
            entry["name"] for entry in expected
        ], f"names read from {source_type}'s .fileCacheInfo do not match"
        assert sizes == [entry["size"] for entry in expected]

        # Exact, not approximate: the maker stamped fixed doubles precisely so
        # a decoding error cannot hide inside a tolerance.
        assert last_access == [entry["lastAccess"] for entry in expected], (
            f"last-access times from {source_type} decoded wrongly; these are "
            f"raw little-endian doubles and eviction orders on them"
        )

        for entry in expected:
            assert cache.is_file(entry["name"])
            path = cache.full_path(entry["name"])
            assert os.path.isfile(path), f"{entry['name']} is indexed but missing"
            with open(path, "rb") as handle:
                assert handle.read() == bytes(
                    entry["bytes"]
                ), f"wrong bytes for {entry['name']} from {source_type}"
