"""Shared constants and fixtures for DID symmetry tests."""

import os
import tempfile

import pytest

SYMMETRY_BASE = os.path.join(tempfile.gettempdir(), "DID", "symmetryTest")
PYTHON_ARTIFACTS = os.path.join(SYMMETRY_BASE, "pythonArtifacts")
MATLAB_ARTIFACTS = os.path.join(SYMMETRY_BASE, "matlabArtifacts")
SOURCE_TYPES = ["matlabArtifacts", "pythonArtifacts"]


def missing_artifact(message):
    """Fail when cross-language artifacts are required, otherwise skip.

    These conditions used to unconditionally ``pytest.skip()``, which exits 0 --
    so a symmetry job that produced NO cross-language artifacts (e.g. MATLAB's
    tempdir diverged from Python's and matlabArtifacts was never written)
    passed green having compared nothing across languages. In the symmetry.yml
    read_artifacts step the artifacts ARE produced by an earlier step in the
    same job (MATLAB makeArtifacts in Step 1, Python makeArtifacts in Step 2),
    so their absence is a real failure; that step sets
    ``DID_SYMMETRY_REQUIRE_ARTIFACTS`` and this helper fails hard. Under a plain
    ``pytest`` run (python-package.yml, local dev) the OTHER language's
    artifacts legitimately do not exist, so skip as before.

    Lives here rather than in one test module so every read_artifacts test is
    covered by the same gate: when it was local to test_build_database.py, the
    other four modules still skipped green and the honesty gate had holes.
    """
    if os.environ.get("DID_SYMMETRY_REQUIRE_ARTIFACTS"):
        pytest.fail(message)
    pytest.skip(message)


def pytest_collection_modifyitems(config, items):
    """Auto-apply symmetry markers based on test file path."""
    for item in items:
        path = str(item.fspath)
        if "symmetry" in path:
            item.add_marker(pytest.mark.symmetry)
        if os.path.join("make_artifacts", "") in path:
            item.add_marker(pytest.mark.make_artifacts)
        if os.path.join("read_artifacts", "") in path:
            item.add_marker(pytest.mark.read_artifacts)


@pytest.fixture(params=SOURCE_TYPES)
def source_type(request):
    """Parameterized fixture that yields each artifact source type."""
    return request.param
