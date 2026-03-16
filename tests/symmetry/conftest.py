"""Shared constants and fixtures for DID symmetry tests."""

import os
import tempfile

import pytest

SYMMETRY_BASE = os.path.join(tempfile.gettempdir(), "DID", "symmetryTest")
PYTHON_ARTIFACTS = os.path.join(SYMMETRY_BASE, "pythonArtifacts")
MATLAB_ARTIFACTS = os.path.join(SYMMETRY_BASE, "matlabArtifacts")
SOURCE_TYPES = ["matlabArtifacts", "pythonArtifacts"]


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
