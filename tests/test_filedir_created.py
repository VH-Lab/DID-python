"""FileDir exists from database creation, as it does in DID-MATLAB.

``_file_dir()`` only computes the path. Until the constructor created it,
the directory appeared on first ingest rather than at database creation, so
a database that had never stored a file had no ``files/`` at all -- and its
directory listing differed from the MATLAB equivalent, which the
cross-language symmetry tests compare.

DID-MATLAB's sqlitedb constructor::

    cacheDir = fullfile(cacheDir_parent, 'files');
    if ~isfolder(cacheDir)
        mkdir(cacheDir);
    end
"""

from __future__ import annotations

import os

from did.implementations.sqlitedb import SQLiteDB


def _db(tmp_path):
    return SQLiteDB(str(tmp_path / "did-sqlite.sqlite"))


def test_filedir_exists_after_construction(tmp_path):
    db = _db(tmp_path)
    assert os.path.isdir(db._file_dir())


def test_filedir_is_files_beside_the_database(tmp_path):
    db = _db(tmp_path)
    assert db._file_dir() == str(tmp_path / "files")


def test_directory_listing_matches_matlab(tmp_path):
    """A fresh database directory holds the database and files/, nothing else."""
    _db(tmp_path)
    assert sorted(os.listdir(tmp_path)) == ["did-sqlite.sqlite", "files"]


def test_reopening_an_existing_database_is_fine(tmp_path):
    """The create is idempotent; reopening must not fail on an existing dir."""
    _db(tmp_path)
    db2 = _db(tmp_path)
    assert os.path.isdir(db2._file_dir())


def test_filedir_survives_being_removed_and_reopened(tmp_path):
    """A database whose files/ was deleted gets it back on next open.

    This is what lets databases created before this change pick the
    directory up, rather than only newly created ones.
    """
    db = _db(tmp_path)
    os.rmdir(db._file_dir())
    assert not os.path.isdir(db._file_dir())
    db2 = _db(tmp_path)
    assert os.path.isdir(db2._file_dir())
