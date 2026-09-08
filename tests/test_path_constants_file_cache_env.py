"""``PathConstants.filecachepath`` honours ``DID_FILE_CACHE_PATH``.

The file cache is otherwise machine-global (``~/Documents/DID/fileCache``).
That is unremarkable in production but a hazard in downstream test suites,
because tests keyed on a constant uid warm the cache once and then quietly
resolve from it forever after -- the retrieval they exist to check silently
stops happening while they keep reporting success (NDI-python#261). This
env var is the supported way to redirect the cache without a downstream
having to reach into ``PathConstants._file_cache_path`` or the memoized
``get_cache`` handle.
"""

from __future__ import annotations

import pytest

from did.common import PathConstants


@pytest.fixture
def clear_env(monkeypatch):
    monkeypatch.delenv(PathConstants.FILE_CACHE_ENV, raising=False)


def test_default_when_env_var_unset(clear_env, tmp_path, monkeypatch):
    # Use a tmp default so the assertion doesn't touch the developer's home.
    monkeypatch.setattr(PathConstants, "_file_cache_path", str(tmp_path / "default"))
    assert PathConstants().filecachepath == str(tmp_path / "default")


def test_env_var_overrides_class_default(tmp_path, monkeypatch):
    override = tmp_path / "override"
    monkeypatch.setattr(PathConstants, "_file_cache_path", str(tmp_path / "default"))
    monkeypatch.setenv(PathConstants.FILE_CACHE_ENV, str(override))
    assert PathConstants().filecachepath == str(override)


def test_empty_env_var_falls_back_to_default(tmp_path, monkeypatch):
    monkeypatch.setattr(PathConstants, "_file_cache_path", str(tmp_path / "default"))
    monkeypatch.setenv(PathConstants.FILE_CACHE_ENV, "")
    assert PathConstants().filecachepath == str(tmp_path / "default")


def test_env_var_is_read_on_every_access(tmp_path, monkeypatch):
    monkeypatch.setattr(PathConstants, "_file_cache_path", str(tmp_path / "default"))
    a = tmp_path / "a"
    b = tmp_path / "b"
    monkeypatch.setenv(PathConstants.FILE_CACHE_ENV, str(a))
    assert PathConstants().filecachepath == str(a)
    monkeypatch.setenv(PathConstants.FILE_CACHE_ENV, str(b))
    assert PathConstants().filecachepath == str(b)
