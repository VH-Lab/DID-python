"""did.file's lock files, and reading the ones MATLAB writes.

These matter across languages. DID-matlab locks the same files under the
same "<file>-lock" name, and once both languages share a file cache they
contend for the same lock. A reader that understands only its own expiry
format cannot tell an expired lock from an unreadable one, so a process
that died holding the lock would shut the other language out permanently --
exactly the crash the one-hour expiry exists to recover from.
"""

import datetime as dt
import os

import pytest

from did.file import checkout_lock_file, parse_lock_expiration, release_lock_file


@pytest.fixture
def lock_path(tmp_path):
    return str(tmp_path / "thing.bin-lock")


def write_lock(path, expiration_text, key, trailing_newline=False):
    with open(path, "w") as handle:
        handle.write(f"{expiration_text}\n{key}")
        if trailing_newline:
            handle.write("\n")


def offset(hours):
    # Naive UTC, which is what the lock files carry and what _utcnow()
    # compares against.
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) + dt.timedelta(
        hours=hours
    )


def iso(when):
    return when.isoformat()


def matlab_char_datetime(when):
    """The form MATLAB's char(datetime(...)) produces: 29-Aug-2026 14:35:12."""
    months = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    return (
        f"{when.day:02d}-{months[when.month - 1]}-{when.year} "
        f"{when.hour:02d}:{when.minute:02d}:{when.second:02d}"
    )


class TestParseLockExpiration:
    def test_iso_is_parsed(self):
        assert parse_lock_expiration(
            "2026-08-29T14:35:12.123456"
        ) == dt.datetime(  # noqa: DTZ001
            2026, 8, 29, 14, 35, 12, 123456
        )

    def test_iso_without_fractional_seconds_is_parsed(self):
        assert parse_lock_expiration(
            "2026-08-29T14:35:12"
        ) == dt.datetime(  # noqa: DTZ001
            2026, 8, 29, 14, 35, 12
        )

    def test_matlabs_char_datetime_form_is_parsed(self):
        # What DID-matlab wrote before it moved to ISO 8601, and what any
        # older DID-matlab still writes.
        assert parse_lock_expiration(
            "29-Aug-2026 14:35:12"
        ) == dt.datetime(  # noqa: DTZ001
            2026, 8, 29, 14, 35, 12
        )

    def test_a_trailing_z_is_accepted(self):
        # Valid ISO 8601, and what a MATLAB writer using a UTCLeapSeconds
        # datetime must emit. fromisoformat only reads it from 3.11, and
        # this package supports 3.10.
        assert parse_lock_expiration(
            "2026-08-29T14:35:12.123456Z"
        ) == dt.datetime(  # noqa: DTZ001
            2026, 8, 29, 14, 35, 12, 123456
        )
        assert parse_lock_expiration(
            "2026-08-29T14:35:12Z"
        ) == dt.datetime(  # noqa: DTZ001
            2026, 8, 29, 14, 35, 12
        )

    def test_a_single_digit_day_is_parsed(self):
        assert parse_lock_expiration(
            "1-Sep-2026 09:05:00"
        ) == dt.datetime(  # noqa: DTZ001
            2026, 9, 1, 9, 5, 0
        )

    def test_surrounding_whitespace_is_ignored(self):
        # readlines() hands over the newline.
        assert parse_lock_expiration(
            "  29-Aug-2026 14:35:12\n"
        ) == dt.datetime(  # noqa: DTZ001
            2026, 8, 29, 14, 35, 12
        )

    def test_the_month_is_not_read_in_the_readers_locale(self):
        # The month in MATLAB's older form is always an English abbreviation
        # wherever it was written, so parsing must not go through %b.
        assert parse_lock_expiration("29-Dec-2026 00:00:00").month == 12

    @pytest.mark.parametrize(
        "text", ["", "not a time", "29-Xyz-2026 14:35:12", "2026-13-45T99:99:99"]
    )
    def test_nonsense_is_rejected(self, text):
        with pytest.raises(ValueError):
            parse_lock_expiration(text)


class TestCheckoutAndRelease:
    def test_what_we_write_is_what_we_can_read(self, lock_path):
        _, key = checkout_lock_file(lock_path)
        assert os.path.isfile(lock_path)
        with open(lock_path) as handle:
            lines = handle.readlines()
        assert len(lines) >= 2
        parse_lock_expiration(lines[0])  # must not raise
        assert release_lock_file(lock_path, key) is True
        assert not os.path.exists(lock_path)

    def test_we_write_iso_8601(self, lock_path):
        # Not a locale-dependent rendering: DID-matlab has to read this.
        _, key = checkout_lock_file(lock_path)
        with open(lock_path) as handle:
            first = handle.readline().strip()
        assert dt.datetime.fromisoformat(first)
        release_lock_file(lock_path, key)

    def test_the_lock_file_is_the_name_we_are_given(self, lock_path):
        # Not "<name>.lock". MATLAB checks out "<file>-lock", and the caller
        # passes that whole name; appending anything means the two languages
        # take different locks and exclude each other not at all.
        _, key = checkout_lock_file(lock_path)
        assert os.path.isfile(lock_path)
        assert not os.path.exists(lock_path + ".lock")
        release_lock_file(lock_path, key)

    def test_an_expired_lock_is_reclaimed(self, lock_path):
        write_lock(lock_path, iso(offset(-1)), "someoneElsesKey")
        _, key = checkout_lock_file(lock_path, check_loops=5, throw_error=False)
        assert key, "an expired lock must be reclaimable"
        release_lock_file(lock_path, key)

    def test_an_expired_lock_in_matlabs_format_is_reclaimed(self, lock_path):
        # The case that was broken. fromisoformat cannot read this, the
        # ValueError was swallowed, and the lock read as one that never
        # expires -- so a MATLAB process that died holding it locked Python
        # out of the shared cache permanently.
        write_lock(
            lock_path,
            matlab_char_datetime(offset(-1)),
            "3ff0000000000000_3fe0000000000000",  # num2hex(now)_num2hex(rand)
            trailing_newline=True,
        )
        _, key = checkout_lock_file(lock_path, check_loops=5, throw_error=False)
        assert key, "Python must be able to expire a lock DID-matlab left behind"
        release_lock_file(lock_path, key)

    def test_a_live_lock_is_not_stolen(self, lock_path):
        write_lock(lock_path, iso(offset(1)), "someoneElsesKey")
        _, key = checkout_lock_file(lock_path, check_loops=1, throw_error=False)
        assert key is None
        assert os.path.isfile(lock_path), "and it must not be deleted"

    def test_a_live_lock_in_matlabs_format_is_not_stolen(self, lock_path):
        write_lock(lock_path, matlab_char_datetime(offset(1)), "someoneElsesKey")
        _, key = checkout_lock_file(lock_path, check_loops=1, throw_error=False)
        assert key is None
        assert os.path.isfile(lock_path)

    def test_an_unreadable_lock_is_not_stolen(self, lock_path):
        # Fail safe: an expiry we cannot parse means wait, not take.
        write_lock(lock_path, "whatever this is", "someoneElsesKey")
        _, key = checkout_lock_file(lock_path, check_loops=1, throw_error=False)
        assert key is None
        assert os.path.isfile(lock_path)

    def test_failing_to_lock_can_raise(self, lock_path):
        write_lock(lock_path, iso(offset(1)), "someoneElsesKey")
        with pytest.raises(OSError, match="Unable to obtain lock"):
            checkout_lock_file(lock_path, check_loops=1, throw_error=True)

    def test_the_wrong_key_does_not_release(self, lock_path):
        write_lock(lock_path, iso(offset(1)), "someoneElsesKey")
        assert release_lock_file(lock_path, "myKey") is False
        assert os.path.isfile(lock_path)
