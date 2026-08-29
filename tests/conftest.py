"""Test-wide fixtures.

The one thing here is keeping the file cache out of the developer's home
directory. did.common.get_cache() opens a cache at PathConstants.
filecachepath, which is ~/Documents/DID/fileCache, and sqlitedb.open_doc
consults it -- so without this every test run would write into, and evict
from, the real cache of whoever ran the tests.
"""

import pytest

from did import common

# Captured at import, before the fixture below has patched anything. The
# path-agreement symmetry test has to record where the cache really lives,
# not where the tests redirect it -- comparing two redirected paths would
# prove nothing about whether the two languages agree.
REAL_FILE_CACHE_PATH = common.PathConstants._file_cache_path


@pytest.fixture
def real_file_cache_path():
    """Where the file cache actually lives, ignoring the test redirect."""
    return REAL_FILE_CACHE_PATH


@pytest.fixture(autouse=True)
def isolated_file_cache(tmp_path_factory, monkeypatch):
    """Point the file cache at a fresh temporary directory for each test."""
    cache_dir = tmp_path_factory.mktemp("fileCache")
    monkeypatch.setattr(
        common.PathConstants, "_file_cache_path", str(cache_dir), raising=False
    )
    # get_cache() memoizes, so the singleton has to be dropped as well or a
    # later test would keep using the first test's directory.
    monkeypatch.setattr(common, "_cached_cache", None, raising=False)
    yield
    common._cached_cache = None
