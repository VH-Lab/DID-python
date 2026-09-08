"""did.file.FileCache -- the cache MATLAB's do_open_doc consults on every open.

The format tests matter as much as the behaviour ones. Both languages call
this index ".fileCacheInfo" in the same directory, so a Python cache that
merely works on its own terms is worse than no cache at all: it would
corrupt a MATLAB one.
"""

import datetime as dt
import os
import struct

import pytest

from did.common import get_cache
from did.file import FileCache, datenum, datenum_to_datetime

NAME_CHARACTERS = 33
HEADER_SIZE = 26


def name_of(index):
    return f"{index:033d}"


def make_source(tmp_path, index, size):
    path = tmp_path / f"source_{index}"
    path.write_bytes(bytes([index % 256]) * size)
    return str(path)


@pytest.fixture
def cache_dir(tmp_path):
    directory = tmp_path / "cache"
    directory.mkdir()
    return directory


@pytest.fixture
def cache(cache_dir):
    return FileCache(str(cache_dir), NAME_CHARACTERS, 100000, 80000)


class TestDatenum:
    # The datetimes below are naive on purpose: a datenum is a local-time
    # calendar number, matching MATLAB's now(), so a tz-aware value would be
    # the wrong input. Hence the DTZ001 suppressions.

    def test_it_matches_matlabs_datenum(self):
        # datenum('01-Jan-2000') is 730486 and datenum('01-Jan-1970') is
        # 719529 in MATLAB. The cache stores these numbers as raw doubles,
        # so an offset error would silently reorder every eviction.
        assert datenum(dt.datetime(2000, 1, 1)) == 730486.0  # noqa: DTZ001
        assert datenum(dt.datetime(1970, 1, 1)) == 719529.0  # noqa: DTZ001

    def test_the_time_of_day_is_the_fraction(self):
        assert datenum(dt.datetime(2000, 1, 1, 12)) == 730486.5  # noqa: DTZ001

    def test_it_round_trips(self):
        when = dt.datetime(2020, 6, 15, 13, 45, 30)  # noqa: DTZ001
        assert abs((datenum_to_datetime(datenum(when)) - when).total_seconds()) < 0.001


class TestOnDiskFormat:
    def test_the_index_is_matlabs_binary_layout_not_json(self, cache):
        info = os.path.join(cache.directory_name, FileCache.CACHE_INFO_FILE_NAME)
        with open(info, "rb") as handle:
            raw = handle.read()
        assert len(raw) == HEADER_SIZE
        assert struct.unpack("<H", raw[0:2])[0] == NAME_CHARACTERS
        assert struct.unpack("<QQQ", raw[2:26]) == (100000, 80000, 0)
        assert not raw.lstrip().startswith(b"{")  # it used to write JSON

    def test_a_row_written_by_matlab_reads_back(self, cache_dir, tmp_path):
        # Build .fileCacheInfo exactly as MATLAB's binaryTable would, without
        # going through FileCache, then open it with FileCache.
        info = cache_dir / FileCache.CACHE_INFO_FILE_NAME
        raw = struct.pack("<H", NAME_CHARACTERS) + struct.pack(
            "<QQQ", 50000, 40000, 300
        )
        for index, (size, when) in enumerate([(100, 730486.0), (200, 730487.5)]):
            raw += name_of(index).encode("latin-1")
            raw += struct.pack("<d", when)
            raw += struct.pack("<Q", size)
        info.write_bytes(raw)

        cache = FileCache(str(cache_dir))
        assert cache.file_name_characters == NAME_CHARACTERS
        assert cache.max_size == 50000
        assert cache.reduce_size == 40000
        assert cache.get_properties()["currentSize"] == 300

        names, sizes, last_access = cache.file_list()
        assert names == [name_of(0), name_of(1)]
        assert sizes == [100, 200]
        assert last_access == [730486.0, 730487.5]
        assert cache.is_file(name_of(1))

    def test_the_index_stays_sorted_by_name(self, cache, tmp_path):
        for index in [5, 1, 9, 3]:
            cache.add_file(make_source(tmp_path, index, 10), name_of(index))
        names, _, _ = cache.file_list()
        assert names == sorted(names)


class TestSettings:
    def test_reopening_keeps_the_stored_settings(self, cache, cache_dir):
        reopened = FileCache(str(cache_dir))
        assert reopened.max_size == 100000
        assert reopened.reduce_size == 80000
        assert reopened.file_name_characters == NAME_CHARACTERS

    def test_reopening_can_change_the_size_limits(self, cache, cache_dir):
        reopened = FileCache(str(cache_dir), NAME_CHARACTERS, 60000, 50000)
        assert reopened.get_properties()["maxSize"] == 60000
        assert FileCache(str(cache_dir)).max_size == 60000

    def test_the_name_width_cannot_be_changed(self, cache, cache_dir):
        # Every row is fixed-width, so a different width reinterprets the
        # whole file.
        with pytest.raises(ValueError, match="may not be altered"):
            FileCache(str(cache_dir), 40)

    def test_reduce_size_must_be_below_max_size(self, cache_dir):
        with pytest.raises(ValueError, match="must be less than"):
            FileCache(str(cache_dir), NAME_CHARACTERS, 1000, 2000)

    def test_a_missing_directory_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="existing directory"):
            FileCache(str(tmp_path / "nope"))


class TestContents:
    def test_add_then_find(self, cache, tmp_path):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        assert cache.is_file(name_of(1))
        assert os.path.isfile(cache.full_path(name_of(1)))
        assert cache.get_properties()["currentSize"] == 40

    def test_adding_moves_the_original_by_default(self, cache, tmp_path):
        source = make_source(tmp_path, 1, 40)
        cache.add_file(source, name_of(1))
        assert not os.path.exists(source)

    def test_adding_with_copy_leaves_the_original(self, cache, tmp_path):
        source = make_source(tmp_path, 1, 40)
        cache.add_file(source, name_of(1), copy=True)
        assert os.path.exists(source)

    def test_the_content_survives(self, cache, tmp_path):
        source = make_source(tmp_path, 7, 40)
        cache.add_file(source, name_of(7))
        with open(cache.full_path(name_of(7)), "rb") as handle:
            assert handle.read() == bytes([7]) * 40

    def test_a_name_of_the_wrong_length_is_refused(self, cache, tmp_path):
        with pytest.raises(ValueError, match="wrong number of characters"):
            cache.add_file(make_source(tmp_path, 1, 40), "short")

    def test_adding_the_same_name_twice_is_refused(self, cache, tmp_path):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        with pytest.raises(ValueError, match="already a file"):
            cache.add_file(make_source(tmp_path, 2, 40), name_of(1))

    def test_remove(self, cache, tmp_path):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        cache.add_file(make_source(tmp_path, 2, 60), name_of(2))
        cache.remove_file(name_of(1))
        assert not cache.is_file(name_of(1))
        assert not os.path.exists(cache.full_path(name_of(1)))
        assert cache.file_list()[0] == [name_of(2)]
        assert cache.get_properties()["currentSize"] == 60

    def test_removing_what_is_not_there_is_refused(self, cache):
        with pytest.raises(ValueError, match="not in file cache manifest"):
            cache.remove_file(name_of(1))

    def test_clear(self, cache, tmp_path):
        for index in range(3):
            cache.add_file(make_source(tmp_path, index, 40), name_of(index))
        cache.clear()
        assert cache.file_list() == ([], [], [])
        assert cache.get_properties()["currentSize"] == 0
        assert os.listdir(cache.directory_name) == [FileCache.CACHE_INFO_FILE_NAME]

    def test_file_list_can_read_the_directory_instead_of_the_index(
        self, cache, tmp_path
    ):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        names, sizes, last_access = cache.file_list(use_catalog=False)
        assert names == [name_of(1)]
        assert sizes == [40]
        assert last_access[0] != last_access[0]  # NaN: unknown from a listing


class TestTouch:
    def test_touching_updates_the_access_time(self, cache, tmp_path):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        before = cache.file_list()[2][0]
        cache.touch(name_of(1))
        assert cache.file_list()[2][0] >= before

    def test_touching_returns_whether_the_file_was_known(self, cache, tmp_path):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        assert cache.touch(name_of(1)) is True
        assert cache.touch(name_of(2)) is False


class TestEviction:
    def test_the_least_recently_used_files_go_first(self, cache_dir, tmp_path):
        cache = FileCache(str(cache_dir), NAME_CHARACTERS, 100000, 80000)
        for index in range(2):
            cache.add_file(make_source(tmp_path, index, 40000), name_of(index))
        # 40000 more would be 120000, over the 100000 cap, so the cache is
        # reduced to 80000 -- which costs exactly one file, the older one.
        cache.add_file(make_source(tmp_path, 2, 40000), name_of(2))

        names, _, _ = cache.file_list()
        assert names == [name_of(1), name_of(2)]
        assert not os.path.exists(cache.full_path(name_of(0)))
        assert cache.get_properties()["currentSize"] == 80000

    def test_touching_a_file_saves_it_from_eviction(self, cache_dir, tmp_path):
        cache = FileCache(str(cache_dir), NAME_CHARACTERS, 100000, 80000)
        for index in range(2):
            cache.add_file(make_source(tmp_path, index, 40000), name_of(index))
        cache.touch(name_of(0))  # the older file is now the freshest
        cache.add_file(make_source(tmp_path, 2, 40000), name_of(2))

        names, _, _ = cache.file_list()
        assert name_of(0) in names
        assert name_of(1) not in names

    def test_a_file_larger_than_the_whole_cache_is_refused(self, cache, tmp_path):
        with pytest.raises(ValueError, match="exceed cache allowed size"):
            cache.add_file(make_source(tmp_path, 1, 200000), name_of(1))


class TestGetCache:
    def test_it_returns_a_working_cache(self, tmp_path):
        # get_cache() used to return a three-line placeholder from
        # did/common.py that held a path and a number and could not cache
        # anything. There is now one FileCache and this is it.
        cache = get_cache()
        assert isinstance(cache, FileCache)
        assert cache.file_name_characters == 33  # the length of a did unique id

        source = tmp_path / "source"
        source.write_bytes(b"payload")
        cache.add_file(str(source), name_of(4))
        assert cache.is_file(name_of(4))

    def test_it_is_the_same_object_every_time(self):
        assert get_cache() is get_cache()

    def test_reset_returns_a_fresh_cache(self):
        first = get_cache()
        second = get_cache("reset")
        assert isinstance(second, FileCache)
        assert first is not second, "reset should drop the memoized handle"
        third = get_cache()
        assert third is second, "the new cache is memoized until the next reset"

    def test_reset_rejects_an_unknown_action(self):
        with pytest.raises(ValueError, match='"reset"'):
            get_cache("clear")


class TestStaleRowReconciliation:
    """add_file tolerates an index row whose file has vanished.

    An interrupted eviction, a failed move, or a lock race can leave the
    index promising a file that is no longer on disk. Without this the
    next add_file of the same uid would refuse as "already in cache" and
    the uid would be permanently poisoned. Mirrors MATLAB fileCache
    (DID-matlab 4a0f9a5). See DID-python#66.
    """

    def test_add_file_replaces_a_stale_row_instead_of_refusing(
        self, cache, tmp_path
    ):
        first = make_source(tmp_path, 1, 40)
        cache.add_file(first, name_of(1))
        # Simulate an interrupted eviction: index row is still there but
        # the file itself has vanished from the cache directory.
        os.remove(cache.full_path(name_of(1)))

        second = make_source(tmp_path, 2, 60)
        cache.add_file(second, name_of(1))
        assert os.path.isfile(cache.full_path(name_of(1)))
        assert cache.is_file(name_of(1))

    def test_add_file_still_refuses_a_row_whose_file_is_present(
        self, cache, tmp_path
    ):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        with pytest.raises(ValueError, match="already"):
            cache.add_file(make_source(tmp_path, 2, 60), name_of(1))


class TestErrorPathReleasesLock:
    """A failed add_file must not leave the binary table with has_lock=True.

    Without the reset a subsequent get_lock returns (None, None) and the
    cache is unlocked against itself for the rest of the process.
    Mirrors MATLAB fileCache addFile/clear hardening (DID-matlab 4a0f9a5).
    See DID-python#66.
    """

    def test_a_failed_add_file_leaves_the_lock_reset(self, cache, tmp_path):
        with pytest.raises(FileNotFoundError):
            cache.add_file(str(tmp_path / "does-not-exist"), name_of(1))
        assert cache.binary_table is None or cache.binary_table.has_lock is False

    def test_a_wrong_length_name_still_leaves_the_lock_reset(
        self, cache, tmp_path
    ):
        # The length check fires before the lock is taken, so the lock
        # was never held -- but has_lock must be False either way.
        with pytest.raises(ValueError):
            cache.add_file(make_source(tmp_path, 1, 40), "too-short")
        assert cache.binary_table is None or cache.binary_table.has_lock is False


class TestCheck:
    """FileCache.check reports and optionally repairs index/disk drift.

    Mirrors MATLAB fileCache/check (DID-matlab 4a0f9a5).
    """

    def test_a_healthy_cache_reports_nothing(self, cache, tmp_path):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        report = cache.check()
        assert report["staleRows"] == []
        assert report["orphanFiles"] == []
        assert report["consistent"] == 1
        assert report["repaired"] == {"rowsDropped": 0, "filesDeleted": 0}

    def test_a_stale_row_is_reported_but_not_repaired_by_default(
        self, cache, tmp_path
    ):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        os.remove(cache.full_path(name_of(1)))
        report = cache.check()
        assert report["staleRows"] == [name_of(1)]
        assert report["repaired"]["rowsDropped"] == 0
        # The row is still there.
        row, _ = cache._table().find_row(1, name_of(1))
        assert row > 0

    def test_repair_drops_stale_rows_and_corrects_currentsize(
        self, cache, tmp_path
    ):
        cache.add_file(make_source(tmp_path, 1, 40), name_of(1))
        cache.add_file(make_source(tmp_path, 2, 60), name_of(2))
        before = cache.get_properties()["currentSize"]
        os.remove(cache.full_path(name_of(1)))

        report = cache.check(repair=True)
        assert report["repaired"]["rowsDropped"] == 1
        row, _ = cache._table().find_row(1, name_of(1))
        assert row == 0
        assert cache.get_properties()["currentSize"] == before - 40

    def test_an_orphan_file_is_reported_but_left_alone_by_default(
        self, cache
    ):
        orphan = cache.full_path(name_of(9))
        with open(orphan, "wb") as handle:
            handle.write(b"stray bytes")

        report = cache.check()
        assert report["orphanFiles"] == [name_of(9)]
        assert os.path.isfile(orphan)

    def test_remove_orphans_deletes_the_files_it_reports(self, cache):
        orphan = cache.full_path(name_of(9))
        with open(orphan, "wb") as handle:
            handle.write(b"stray bytes")

        report = cache.check(remove_orphans=True)
        assert report["repaired"]["filesDeleted"] == 1
        assert not os.path.exists(orphan)


class TestBinaryTableResetLockState:
    """reset_lock_state clears the in-memory flag without touching disk."""

    def test_it_forces_has_lock_back_to_false(self, cache, tmp_path):
        table = cache._table()
        _fid, key = table.get_lock()
        try:
            assert table.has_lock is True
            table.reset_lock_state()
            assert table.has_lock is False
        finally:
            # Release the on-disk lock even though the in-memory flag is
            # already reset: this is what a well-behaved caller does.
            from did.file import release_lock_file

            if key:
                release_lock_file(table.lock_file_name(), key)
