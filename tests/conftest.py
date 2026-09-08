"""Test-wide fixtures, and the backstop against a bridge check that skips.

Keeping the file cache out of the developer's home directory is the main
fixture here. did.common.get_cache() opens a cache at PathConstants.
filecachepath, which is ~/Documents/DID/fileCache, and sqlitedb.open_doc
consults it -- so without this every test run would write into, and evict
from, the real cache of whoever ran the tests.

The hook below is the other thing: under DID_BRIDGE_CHECK_STRICT, a skip
inside a bridge test module is turned into a failure.
"""

import os

import pytest

from did import common

# Modules whose skips are suppressed under strict mode. DID's bridge suite is
# tests/test_bridge_contract.py; the glob leaves room for a second file without
# needing this list updated, which is the kind of upkeep that gets forgotten.
BRIDGE_TEST_PREFIX = "test_bridge_"


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    """Under strict mode, a skipped bridge test fails instead.

    Every individual skip looks reasonable where it is written -- the inputs
    are missing, the clone is shallow, the tool is absent. What makes them
    dangerous is collective: a skip exits 0, so a suite that skipped the check
    and a suite that ran it and passed are indistinguishable from the outside.
    NDR-python hit this three times (their #25): a bridge job naming one test
    file, 79 skips a matrix job, and three ungated skips inside the drift
    self-tests -- the positive controls for the drift rule itself, quietly not
    running.

    So the rule is not "write your skips carefully", which is what the three
    occurrences already were. It is that in the environment where the inputs
    are guaranteed -- CI, where DID_BRIDGE_CHECK_STRICT is set -- a skip is a
    bug by definition, and the harness says so without depending on whoever
    wrote the skip having thought of it.

    This is a backstop, not the primary mechanism: a skip should still be
    written conditionally, so it reads honestly when run locally. It catches
    the ones nobody thought about, including skips from markers, fixtures and
    importorskip that no amount of care at the call site would gate.
    """
    outcome = yield
    report = outcome.get_result()
    if report.when != "call" or not report.skipped:
        return
    if not os.environ.get("DID_BRIDGE_CHECK_STRICT"):
        return
    if not os.path.basename(str(item.path)).startswith(BRIDGE_TEST_PREFIX):
        return

    reason = ""
    if isinstance(report.longrepr, tuple) and len(report.longrepr) == 3:
        reason = report.longrepr[2]
    report.outcome = "failed"
    report.longrepr = (
        f"{item.nodeid} SKIPPED under DID_BRIDGE_CHECK_STRICT: {reason}\n\n"
        "A bridge check that does not run must not report as one that passed. "
        "Strict mode is set in CI, where this check's inputs are guaranteed, so "
        "a skip here means something broke -- fix the cause, or gate the skip "
        "on the env var so it only skips where the inputs really can be absent."
    )


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
