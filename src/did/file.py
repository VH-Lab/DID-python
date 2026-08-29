import json
import math
import os
import re
import shutil
import struct
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import portalocker


def _utcnow():
    """Naive UTC timestamp, identical to the deprecated datetime.utcnow()."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def must_be_valid_permission(value):
    VALID_PERMISSIONS = [
        "r",
        "w",
        "a",
        "r+",
        "w+",
        "a+",
        "rb",
        "wb",
        "ab",
        "r+b",
        "w+b",
        "a+b",
    ]
    if value not in VALID_PERMISSIONS:
        raise ValueError(
            f"File permission must be one of: {', '.join(VALID_PERMISSIONS)}"
        )


def must_be_valid_machine_format(value):
    VALID_MACHINE_FORMAT = [
        "n",
        "native",
        "b",
        "ieee-be",
        "l",
        "ieee-le",
        "s",
        "ieee-be.l64",
        "a",
        "ieee-le.l64",
    ]
    if value not in VALID_MACHINE_FORMAT:
        raise ValueError(
            f"Machine format must be one of: {', '.join(VALID_MACHINE_FORMAT)}"
        )


class Fileobj:
    def __init__(self, fullpathfilename="", permission="r", machineformat="n"):
        must_be_valid_permission(permission)
        must_be_valid_machine_format(machineformat)
        self.fullpathfilename = fullpathfilename
        self.permission = permission
        self.machineformat = machineformat
        self.fid = None

    def set_properties(
        self, fullpathfilename=None, permission=None, machineformat=None
    ):
        if fullpathfilename:
            self.fullpathfilename = fullpathfilename
        if permission:
            must_be_valid_permission(permission)
            self.permission = permission
        if machineformat:
            must_be_valid_machine_format(machineformat)
            self.machineformat = machineformat
        return self

    def fopen(self, permission=None, machineformat=None, filename=None):
        if self.fid:
            self.fclose()

        if permission:
            self.set_properties(permission=permission)
        if machineformat:
            self.set_properties(machineformat=machineformat)
        if filename:
            self.set_properties(fullpathfilename=filename)

        try:
            # Python's open() doesn't have a direct machine format mapping like Matlab.
            # The 'b' for binary mode is the most relevant part of the permission string.
            mode = self.permission
            if "b" not in mode:
                mode += "b"  # Default to binary for this class

            # Not a context manager: the handle is owned by this object and
            # stays open until fclose(), mirroring MATLAB's fopen/fclose.
            self.fid = open(self.fullpathfilename, mode)  # noqa: SIM115
        except OSError:
            self.fid = None
        return self

    def fclose(self):
        if getattr(self, "fid", None):
            self.fid.close()
            self.fid = None

    def fseek(self, offset, reference):
        if self.fid:
            return self.fid.seek(offset, reference)
        return -1

    def ftell(self):
        if self.fid:
            return self.fid.tell()
        return -1

    def frewind(self):
        if self.fid:
            self.fid.seek(0)

    def feof(self):
        if self.fid:
            current_pos = self.fid.tell()
            self.fid.seek(0, 2)
            end_pos = self.fid.tell()
            self.fid.seek(current_pos)
            return current_pos == end_pos
        return -1

    def fwrite(self, data):
        if self.fid:
            return self.fid.write(data)
        return 0

    def fread(self, count=-1):
        """Read up to ``count`` bytes, or the rest of the file if count < 0.

        Returns ``bytes``. MATLAB's fread returns ``[data, count]``; Python
        returns the data alone, since ``len(data)`` is the count.

        An unopened file reads as empty, mirroring MATLAB, whose fread returns
        ``data = []`` and ``count = 0`` when fid < 0 rather than raising. Note
        that Fileobj.fopen() likewise does not raise on a missing file -- it
        leaves fid None -- so check fid if you need to tell "empty file" from
        "could not open".
        """
        if self.fid:
            return self.fid.read(count)
        return b""

    def fgetl(self):
        if self.fid:
            line = self.fid.readline()
            return line.strip(b"\n")
        return ""

    def fgets(self, nchar=-1):
        if self.fid:
            return self.fid.readline(nchar)
        return ""

    def ferror(self):
        # Python's file objects raise exceptions rather than setting error flags.
        # This method is for API compatibility.
        return "", 0

    def fileparts(self):
        return os.path.split(self.fullpathfilename)

    def __del__(self):
        self.fclose()


def checkout_lock_file(filename, check_loops=30, throw_error=True, expiration=3600):
    """Try to establish control of the lock file named ``filename``.

    ``filename`` is the lock file itself, not the file being protected --
    the same contract as MATLAB's did.file.checkout_lock_file, whose own
    docstring shows a caller passing "myfile.txt-lock". This used to append
    ".lock" to whatever it was given, which meant two languages guarding one
    file contended for two different lock files and so did not contend at
    all.
    """
    key = f"{_utcnow().isoformat()}_{uuid.uuid4()}"
    lock_filename = filename

    for _ in range(check_loops):
        try:
            # Not a context manager: the handle is returned to the caller,
            # which holds the lock until release_lock_file().
            lock_file = open(lock_filename, "x")  # noqa: SIM115
            # Use portalocker for an exclusive lock
            portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)

            expiration_time = _utcnow() + timedelta(seconds=expiration)
            lock_file.write(f"{expiration_time.isoformat()}\n{key}")
            lock_file.close()  # Close the file handle, but the lock is associated with the file path
            return lock_file, key
        except (OSError, portalocker.exceptions.LockException):
            # File exists or is locked, check for expiration
            try:
                with open(lock_filename, "r") as f:
                    lines = f.readlines()
                    if len(lines) >= 1:
                        expiration_time_str = lines[0].strip()
                        expiration_time = datetime.fromisoformat(expiration_time_str)
                        if _utcnow() > expiration_time:
                            # Lock expired, try to remove it
                            release_lock_file(
                                filename, lines[1].strip() if len(lines) > 1 else ""
                            )
                            continue  # Retry immediately
            except (OSError, ValueError):
                # Could not read lock file or parse time, wait and retry
                pass
            time.sleep(1)

    if throw_error:
        raise OSError(f"Unable to obtain lock with file {filename}.")
    return None, None


def release_lock_file(filename, key):
    """
    Releases the lock file named ``filename`` if ``key`` matches.

    As with checkout_lock_file, ``filename`` is the lock file itself.
    """
    lock_filename = filename
    if not os.path.exists(lock_filename):
        return True

    try:
        with open(lock_filename, "r+") as f:
            portalocker.lock(f, portalocker.LOCK_EX)
            lines = f.readlines()
            if len(lines) >= 2 and lines[1].strip() == key:
                # We have the key, release the lock and delete the file
                f.truncate(0)  # Clear the file
                portalocker.unlock(f)
                os.remove(lock_filename)
                return True
            else:
                # Key doesn't match, don't release
                portalocker.unlock(f)
                return False
    except (OSError, portalocker.exceptions.LockException):
        # Could not get a lock, or file was removed by another process
        return not os.path.exists(lock_filename)


# MATLAB precision strings mapped onto struct format characters and byte
# widths. These are the names that appear in a binaryTable's recordType, and
# they are what makes a table written by one language readable by the other:
# the layout on disk is MATLAB's, not a Python-flavoured approximation of it.
# 'char' is one byte per element, as MATLAB's fwrite(x,'char') writes.
_MATLAB_TYPE_FORMATS = {
    "char": ("B", 1),
    "int8": ("b", 1),
    "uint8": ("B", 1),
    "int16": ("h", 2),
    "uint16": ("H", 2),
    "int32": ("i", 4),
    "uint32": ("I", 4),
    "int64": ("q", 8),
    "uint64": ("Q", 8),
    "single": ("f", 4),
    "float32": ("f", 4),
    "double": ("d", 8),
    "float64": ("d", 8),
}

_COPY_BUFFER_SIZE = 1000000  # 1 MB, as MATLAB's insertRow/deleteRow use


class BinaryTable:
    """A binary table of fixed-width rows, with multi-process locking.

    The on-disk layout is MATLAB's ``did.file.binaryTable``: an optional
    header of ``header_size`` bytes, then rows of ``sum(record_size)`` bytes,
    each row holding one fixed-width value per column. Everything is written
    little-endian regardless of platform, so a table written by either
    language can be read by the other.

    Rows and columns are 1-based, matching MATLAB, because the row numbers
    are part of this class's contract (``find_row`` returns 0 for "not
    found", and ``insert_row`` takes a row to insert *after*, where 0 means
    "at the front").
    """

    def __init__(self, f, record_type, record_size, elements_per_column, header_size):
        self.file = f
        self.record_type = list(record_type)
        self.record_size = [int(s) for s in record_size]
        self.elements_per_column = [int(n) for n in elements_per_column]
        self.header_size = int(header_size)
        self.has_lock = False
        self.file.set_properties(machineformat="l")  # always little-endian

        if not self.file.fullpathfilename:
            raise ValueError("A full path file name must be given to the file object.")

        if not (
            len(self.record_type)
            == len(self.record_size)
            == len(self.elements_per_column)
        ):
            raise ValueError(
                "record_type, record_size and elements_per_column must have "
                "the same number of entries (one per column)."
            )

        for index, type_name in enumerate(self.record_type):
            if type_name not in _MATLAB_TYPE_FORMATS:
                raise ValueError(
                    f'Unsupported record type "{type_name}" for column '
                    f"{index + 1}. Supported types: "
                    f"{', '.join(sorted(_MATLAB_TYPE_FORMATS))}."
                )
            _, element_size = _MATLAB_TYPE_FORMATS[type_name]
            expected = element_size * self.elements_per_column[index]
            if self.record_size[index] != expected:
                raise ValueError(
                    f"Column {index + 1} declares {self.record_size[index]} "
                    f"bytes, but {self.elements_per_column[index]} elements of "
                    f'"{type_name}" occupy {expected} bytes.'
                )

    # -- layout -----------------------------------------------------------

    def get_size(self):
        """Return ``(rows, columns, data_bytes)`` for the table's file."""
        data_size = 0
        if os.path.exists(self.file.fullpathfilename):
            file_size = os.path.getsize(self.file.fullpathfilename)
            data_size = file_size - self.header_size

        c = len(self.record_size)
        row_size = sum(self.record_size)
        r = data_size // row_size if row_size > 0 else 0
        return r, c, data_size

    def row_size(self):
        return sum(self.record_size)

    def _column_offset(self, col):
        """Byte offset of 1-based column ``col`` within a row."""
        return sum(self.record_size[: col - 1])

    def _row_offset(self, row):
        """Byte offset of 1-based row ``row`` from the start of the file."""
        return self.header_size + (row - 1) * self.row_size()

    def _check_column(self, col):
        col = int(col)
        if col < 1 or col > len(self.record_size):
            raise ValueError(
                f"Column must be in 1..number of columns " f"({len(self.record_size)})."
            )
        return col

    # -- value encoding ---------------------------------------------------

    def _pack_column(self, col, value):
        """Encode ``value`` as the bytes of 1-based column ``col``."""
        type_name = self.record_type[col - 1]
        count = self.elements_per_column[col - 1]
        fmt, _ = _MATLAB_TYPE_FORMATS[type_name]

        if type_name == "char":
            if isinstance(value, (bytes, bytearray)):
                text = bytes(value).decode("latin-1")
            else:
                text = str(value)
            if len(text) != count:
                raise ValueError(
                    f"Column {col} holds {count} characters, but "
                    f"{len(text)} were given."
                )
            return text.encode("latin-1")

        if isinstance(value, (str, bytes, bytearray)):
            raise TypeError(f'Column {col} is "{type_name}"; text was given.')
        values = [value] if not isinstance(value, (list, tuple)) else list(value)
        if len(values) != count:
            raise ValueError(
                f"Column {col} holds {count} elements, but "
                f"{len(values)} were given."
            )
        if fmt not in ("f", "d"):
            values = [int(v) for v in values]
        return struct.pack("<" + fmt * count, *values)

    def _unpack_column(self, col, raw):
        """Decode the bytes of 1-based column ``col``."""
        type_name = self.record_type[col - 1]
        count = self.elements_per_column[col - 1]
        fmt, _ = _MATLAB_TYPE_FORMATS[type_name]

        if type_name == "char":
            return raw.decode("latin-1")

        values = struct.unpack("<" + fmt * count, raw)
        return values[0] if count == 1 else list(values)

    def _pack_row(self, data):
        """Encode one row given a value per column."""
        if len(data) != len(self.record_type):
            raise ValueError(
                f"A row has {len(self.record_type)} columns, but "
                f"{len(data)} values were given."
            )
        return b"".join(
            self._pack_column(col, data[col - 1])
            for col in range(1, len(self.record_type) + 1)
        )

    # -- header -----------------------------------------------------------

    def read_header(self):
        """Return the table's header bytes, or ``b""`` if there is no file.

        MATLAB's readHeader on a missing file leaves fid < 0 and fread
        returns empty; this returns empty for the same case rather than
        raising, so a caller can tell "no table yet" from "unreadable".
        """
        lock_fid, key = self.get_lock()
        try:
            if not os.path.isfile(self.file.fullpathfilename):
                return b""
            with open(self.file.fullpathfilename, "rb") as f:
                return f.read(self.header_size)
        finally:
            self.release_lock(lock_fid, key)

    def write_header(self, header_data):
        """Write ``header_data`` over the start of the file.

        Nothing past ``len(header_data)`` is touched -- the rest of the
        header space and every row are left exactly as they were, matching
        MATLAB.
        """
        if isinstance(header_data, str):
            header_data = header_data.encode("latin-1")
        header_data = bytes(header_data)
        if len(header_data) > self.header_size:
            raise ValueError(
                f"Header data to write is larger ({len(header_data)}) than "
                f"the header size of the file ({self.header_size})."
            )

        lock_fid, key = self.get_lock()
        try:
            # "r+b" cannot create a file, and MATLAB picks its permission the
            # same way: 'r+' when the file is there, 'w' when it is not.
            mode = "r+b" if os.path.isfile(self.file.fullpathfilename) else "wb"
            with open(self.file.fullpathfilename, mode) as f:
                f.write(header_data)
                # A fresh file must still be header_size long, or the first
                # row would start early and every offset after it would be
                # wrong. MATLAB gets this from fwrite past end-of-file.
                if f.tell() < self.header_size:
                    f.write(b"\x00" * (self.header_size - f.tell()))
        finally:
            self.release_lock(lock_fid, key)

    # -- locking ----------------------------------------------------------

    def get_lock(self):
        """Take the table's lock, unless this object already holds it.

        Returns ``(lock_fid, key)``; both are None when the lock was already
        held, which is what makes ``release_lock`` a no-op for the nested
        call and leaves the outermost caller owning the lock.
        """
        if not self.has_lock:
            lock_fid, key = checkout_lock_file(self.lock_file_name())
            self.has_lock = True
            return lock_fid, key
        return None, None

    def release_lock(self, lock_fid, key):
        if key:
            release_lock_file(self.lock_file_name(), key)
            self.has_lock = False

    def lock_file_name(self):
        # "-lock", not ".lock": this is the name MATLAB's binaryTable checks
        # out, and two languages sharing a cache directory must contend for
        # the same lock file or the lock protects nothing.
        return f"{self.file.fullpathfilename}-lock"

    def temp_file_name(self):
        return f"{self.file.fullpathfilename}-temp"

    # -- reading ----------------------------------------------------------

    def read_row(self, row, col):
        """Read column ``col`` of one row, several rows, or every row.

        ``row`` is a 1-based row number, a sequence of them, or None (or
        ``math.inf``, as MATLAB spells it) for all rows. A single row number
        returns a single value; anything else returns a list, one entry per
        row requested.

        A value is decoded by the column's record type: a 'char' column
        returns str, a numeric column with one element per row returns a
        number, and a numeric column with several returns a list.
        """
        col = self._check_column(col)

        lock_fid, key = self.get_lock()
        try:
            r, _, _ = self.get_size()

            single = False
            if row is None or (isinstance(row, float) and math.isinf(row)):
                rows = list(range(1, r + 1))
            elif isinstance(row, (list, tuple, range)):
                rows = [int(i) for i in row]
            else:
                rows = [int(row)]
                single = True

            if any(i < 1 or i > r for i in rows):
                raise IndexError(f"Rows must be in 1..{r}.")
            if not rows:
                return [] if not single else None

            width = self.record_size[col - 1]
            offset_in_row = self._column_offset(col)
            out = []
            with open(self.file.fullpathfilename, "rb") as f:
                for i in rows:
                    f.seek(self._row_offset(i) + offset_in_row)
                    raw = f.read(width)
                    if len(raw) != width:
                        raise EOFError(
                            f"Row {i} column {col} is truncated: expected "
                            f"{width} bytes, read {len(raw)}."
                        )
                    out.append(self._unpack_column(col, raw))
            return out[0] if single else out
        finally:
            self.release_lock(lock_fid, key)

    # -- writing ----------------------------------------------------------

    @staticmethod
    def _copy_bytes(source, dest, count):
        copied = 0
        while copied < count:
            chunk = source.read(min(_COPY_BUFFER_SIZE, count - copied))
            if not chunk:
                raise EOFError(
                    f"Expected {count} bytes to copy, ran out after {copied}."
                )
            dest.write(chunk)
            copied += len(chunk)

    def insert_row(self, insert_after, data):
        """Insert a row of data after 1-based row ``insert_after``.

        ``insert_after`` is 0 to put the row at the front and ``rows`` to
        append. It must be in 0..rows: MATLAB's bound check permits rows+1
        and then falls into the copy branch, which writes the new row past
        the end of the data and corrupts the table, so this is stricter on
        purpose.
        """
        insert_after = int(insert_after)
        if insert_after < 0:
            raise ValueError("insert_after must be non-negative.")

        row_bytes = self._pack_row(data)
        r, _, _ = self.get_size()
        if insert_after > r:
            raise ValueError(f"Row must be in 0..number of rows ({r}).")

        lock_fid, key = self.get_lock()
        try:
            if insert_after == r:  # append; no copy needed
                with open(self.file.fullpathfilename, "ab") as f:
                    f.write(row_bytes)
            else:
                before_bytes = self.header_size + insert_after * self.row_size()
                total_bytes = self.header_size + r * self.row_size()
                temp_name = self.temp_file_name()
                with (
                    open(self.file.fullpathfilename, "rb") as source,
                    open(temp_name, "wb") as dest,
                ):
                    self._copy_bytes(source, dest, before_bytes)
                    dest.write(row_bytes)
                    self._copy_bytes(source, dest, total_bytes - before_bytes)
                os.replace(temp_name, self.file.fullpathfilename)
        finally:
            self.release_lock(lock_fid, key)

    def delete_row(self, row):
        """Delete 1-based row ``row``."""
        row = int(row)
        r, _, _ = self.get_size()
        if row < 1 or row > r:
            raise ValueError(f"Row must be in 1..number of rows ({r}).")

        lock_fid, key = self.get_lock()
        try:
            before_bytes = self.header_size + (row - 1) * self.row_size()
            total_bytes = self.header_size + r * self.row_size()
            temp_name = self.temp_file_name()
            with (
                open(self.file.fullpathfilename, "rb") as source,
                open(temp_name, "wb") as dest,
            ):
                self._copy_bytes(source, dest, before_bytes)
                source.seek(self.header_size + row * self.row_size())
                self._copy_bytes(
                    source, dest, total_bytes - before_bytes - self.row_size()
                )
            os.replace(temp_name, self.file.fullpathfilename)
        finally:
            self.release_lock(lock_fid, key)

    def write_entry(self, row, col, value):
        """Overwrite one entry in place."""
        col = self._check_column(col)
        row = int(row)
        raw = self._pack_column(col, value)

        r, _, _ = self.get_size()
        if row < 1 or row > r:
            raise IndexError(f"Row {row} is out of bounds.")

        lock_fid, key = self.get_lock()
        try:
            with open(self.file.fullpathfilename, "r+b") as f:
                f.seek(self._row_offset(row) + self._column_offset(col))
                f.write(raw)
        finally:
            self.release_lock(lock_fid, key)

    def write_table(self, data):
        """Replace every row in the table. The header is carried over.

        ``data`` is a sequence of rows, each a sequence with one value per
        column. Old rows are lost.
        """
        rows = [self._pack_row(row) for row in data]

        lock_fid, key = self.get_lock()
        try:
            header = self.read_header()
            if len(header) < self.header_size:
                header = header + b"\x00" * (self.header_size - len(header))
            temp_name = self.temp_file_name()
            with open(temp_name, "wb") as dest:
                dest.write(header)
                for row_bytes in rows:
                    dest.write(row_bytes)
            os.replace(temp_name, self.file.fullpathfilename)
        finally:
            self.release_lock(lock_fid, key)

    # -- searching --------------------------------------------------------

    def find_row(
        self,
        col,
        value,
        sorted=False,  # noqa: A002 - MATLAB's name for this option
        lower_bound=None,
        upper_bound=None,
        is_recurrent=False,
    ):
        """Find the row where column ``col`` holds ``value``.

        Returns ``(row, would_be)``. ``row`` is the 1-based row, or 0 when
        the value is not present.

        ``would_be`` is only meaningful when ``sorted`` is True, in which
        case it is the row *after which* the value would belong -- exactly
        what ``insert_row`` takes -- and a binary search is used. When
        ``sorted`` is False every row is read in turn and ``would_be`` is
        NaN, as in MATLAB.
        """
        col = self._check_column(col)
        row = 0
        would_be = math.nan

        lock_fid, key = (None, None) if is_recurrent else self.get_lock()
        try:
            if not sorted:
                r, _, _ = self.get_size()
                for i in range(1, r + 1):
                    if self.read_row(i, col) == value:
                        row = i
                        break
                return row, would_be

            r_total, _, _ = self.get_size()
            if lower_bound is None:
                lower_bound = 1
            if upper_bound is None:
                upper_bound = r_total

            r_look = math.floor(lower_bound + (upper_bound - lower_bound) / 2)
            if r_look < 1 or r_look > r_total:
                # Out of bounds, which for an empty table means the value
                # belongs at the front.
                return 0, 0

            comparison = self.compare(self.read_row(r_look, col), value)
            last_move = upper_bound <= lower_bound

            if comparison == 0:
                return r_look, would_be

            if comparison < 0:  # the value here sorts after the one sought
                new_lower, new_upper = lower_bound, r_look - 1
            else:
                new_lower, new_upper = r_look + 1, upper_bound

            if last_move:
                # Nowhere left to look: the value goes just before this row
                # when this row sorts after it, and just after it otherwise.
                return 0, r_look - 1 if comparison < 0 else r_look

            return self.find_row(
                col,
                value,
                sorted=True,
                lower_bound=new_lower,
                upper_bound=new_upper,
                is_recurrent=True,
            )
        finally:
            if not is_recurrent:
                self.release_lock(lock_fid, key)

    @staticmethod
    def compare(value1, value2):
        """Order two values: 1 if value1 < value2, -1 if greater, 0 if equal.

        Numbers compare numerically and text alphabetically. A list or tuple
        is compared by its first entry, as MATLAB compares a cell by its
        first cell.
        """
        if isinstance(value1, (list, tuple)) and value1:
            value1 = value1[0]
        if isinstance(value2, (list, tuple)) and value2:
            value2 = value2[0]
        if isinstance(value1, (bytes, bytearray)):
            value1 = bytes(value1).decode("latin-1")
        if isinstance(value2, (bytes, bytearray)):
            value2 = bytes(value2).decode("latin-1")

        both_text = isinstance(value1, str) and isinstance(value2, str)
        both_numbers = isinstance(value1, (int, float)) and isinstance(
            value2, (int, float)
        )
        if not (both_text or both_numbers):
            raise ValueError("Could not make comparison.")

        if value1 == value2:
            return 0
        return 1 if value1 < value2 else -1


class DumbJsonDB:
    def __init__(
        self,
        command="none",
        filename="",
        dirname=".dumbjsondb",
        unique_object_id_field="id",
    ):
        self.paramfilename = ""
        self.dirname = dirname
        self.unique_object_id_field = unique_object_id_field

        if command == "new":
            self.paramfilename = filename
            self._write_parameters()
        elif command == "load":
            self._load_parameters(filename)

    def _document_path(self):
        p = os.path.dirname(self.paramfilename)
        return os.path.join(p, self.dirname)

    def _write_parameters(self):
        if not self.paramfilename:
            return

        path = os.path.dirname(self.paramfilename)
        if not os.path.exists(path):
            os.makedirs(path)

        params = {
            "dirname": self.dirname,
            "unique_object_id_field": self.unique_object_id_field,
        }
        with open(self.paramfilename, "w") as f:
            json.dump(params, f, indent=4)

        doc_path = self._document_path()
        if not os.path.exists(doc_path):
            os.makedirs(doc_path)

    def _load_parameters(self, filename):
        self.paramfilename = filename
        with open(filename, "r") as f:
            params = json.load(f)
        self.dirname = params.get("dirname", self.dirname)
        self.unique_object_id_field = params.get(
            "unique_object_id_field", self.unique_object_id_field
        )

    @staticmethod
    def _fix_doc_unique_id(doc_unique_id):
        if isinstance(doc_unique_id, (int, float)):
            return str(doc_unique_id)
        return doc_unique_id

    @staticmethod
    def _uniqueid2filename(doc_unique_id, version=0):
        doc_unique_id = DumbJsonDB._fix_doc_unique_id(doc_unique_id)
        # A simple and safe way to create a filename from an ID
        safe_id = "".join(
            [c for c in doc_unique_id if c.isalpha() or c.isdigit() or c == "_"]
        ).rstrip()
        return f"Object_id_{safe_id}_v{version:05x}.json"

    def doc_versions(self, doc_unique_id):
        doc_unique_id = self._fix_doc_unique_id(doc_unique_id)
        path = self._document_path()
        versions = []

        # Simplified version search, a more robust implementation would parse filenames more carefully
        prefix = f"Object_id_{doc_unique_id}_v"
        for f in os.listdir(path):
            if f.startswith(prefix) and f.endswith(".json"):
                try:
                    version_hex = f[len(prefix) : -5]
                    versions.append(int(version_hex, 16))
                except ValueError:
                    continue  # filename format not as expected
        return sorted(versions)

    def read(self, doc_unique_id, version=None):
        doc_unique_id = self._fix_doc_unique_id(doc_unique_id)
        if version is None:
            versions = self.doc_versions(doc_unique_id)
            if not versions:
                return None, None
            version = versions[-1]

        filename = self._uniqueid2filename(doc_unique_id, version)
        filepath = os.path.join(self._document_path(), filename)

        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return json.load(f), version
        return None, None

    def add(self, doc_object, overwrite=1, doc_version=None):
        doc_unique_id = self._fix_doc_unique_id(doc_object[self.unique_object_id_field])

        if doc_version is None:
            versions = self.doc_versions(doc_unique_id)
            doc_version = versions[-1] if versions else 0

        filename = self._uniqueid2filename(doc_unique_id, doc_version)
        filepath = os.path.join(self._document_path(), filename)

        file_exists = os.path.exists(filepath)

        if file_exists:
            if overwrite == 0:
                raise OSError(
                    f"Document with id {doc_unique_id} and version {doc_version} already exists."
                )
            elif overwrite == 2:
                doc_version = max(self.doc_versions(doc_unique_id) or [0]) + 1
                filename = self._uniqueid2filename(doc_unique_id, doc_version)
                filepath = os.path.join(self._document_path(), filename)

        with open(filepath, "w") as f:
            json.dump(doc_object, f, indent=4)

        # Simplified metadata update
        self._update_doc_metadata(
            "Added new version", doc_object, doc_unique_id, doc_version
        )

    def _update_doc_metadata(self, operation, document, doc_unique_id, doc_version):
        # This is a simplified placeholder for the metadata logic.
        # A full implementation would be more complex.
        pass


def datenum(when=None):
    """MATLAB's ``now``: days since the start of the proleptic year 0.

    The file cache stores last-access times in this form because MATLAB
    writes them into ``.fileCacheInfo`` as raw doubles. Reading them as
    anything else would order the cache by a number that is not a date, so
    the conversion belongs here rather than at each call site.
    """
    when = datetime.now() if when is None else when
    seconds = (
        when.hour * 3600 + when.minute * 60 + when.second + when.microsecond / 1000000.0
    )
    # datetime.toordinal() counts from 0001-01-01 == 1; MATLAB counts that
    # same day as 367, a fixed 366-day offset for the year 0.
    return when.toordinal() + 366 + seconds / 86400.0


def datenum_to_datetime(value):
    """Turn a MATLAB datenum back into a datetime. Inverse of ``datenum``."""
    days = math.floor(value) - 366
    seconds = (value - math.floor(value)) * 86400.0
    return datetime.fromordinal(int(days)) + timedelta(seconds=seconds)


class FileCache:
    """A directory of cached files with a size cap and least-recently-used eviction.

    The index lives in the cache directory as ``.fileCacheInfo``, in
    MATLAB's ``did.file.fileCache`` binary layout: a 26-byte header
    (fileNameCharacters as uint16, then maxSize, reduceSize and currentSize
    as uint64) followed by one fixed-width row per file holding the file
    name, its last-access time as a MATLAB datenum, and its size in bytes.
    The rows are kept sorted by name so lookups can binary-search.

    Every file in the cache has a name of exactly ``file_name_characters``
    characters -- DID uses the 33-character unique id of the file's
    location -- which is what makes the rows fixed-width.
    """

    CACHE_INFO_FILE_NAME = ".fileCacheInfo"

    # uint16 fileNameCharacters + uint64 maxSize + reduceSize + currentSize
    HEADER_SIZE = 2 + 8 + 8 + 8

    DEFAULT_FILE_NAME_CHARACTERS = 32
    DEFAULT_MAX_SIZE = 100000000000  # 100 GB
    DEFAULT_REDUCE_SIZE = 80000000000  # 80 GB

    def __init__(
        self,
        directory_name,
        file_name_characters=None,
        max_size=None,
        reduce_size=None,
    ):
        """Open (or create) the cache in an existing directory.

        The optional arguments default to None rather than to their values
        so that "not given" stays distinguishable from "given the default":
        an existing cache keeps its stored settings, and only an argument
        actually passed overrides them. That is what MATLAB's nargin checks
        do, and it is why re-opening a cache does not silently resize it.
        """
        if not os.path.isdir(directory_name):
            raise ValueError(
                f'directory_name must be an existing directory; "{directory_name}" is not.'
            )

        if file_name_characters is not None and int(file_name_characters) < 32:
            raise ValueError("file_name_characters must be at least 32.")

        self.directory_name = directory_name
        self.file_name_characters = (
            self.DEFAULT_FILE_NAME_CHARACTERS
            if file_name_characters is None
            else int(file_name_characters)
        )
        self.max_size = self.DEFAULT_MAX_SIZE if max_size is None else int(max_size)
        self.reduce_size = (
            self.DEFAULT_REDUCE_SIZE if reduce_size is None else int(reduce_size)
        )
        self.current_size = 0
        self.binary_table = None

        need_to_set = True
        if os.path.isfile(self._info_file_name()):
            need_to_set = False
            saved = self.get_properties()
            if file_name_characters is not None and saved["fileNameCharacters"] != int(
                file_name_characters
            ):
                raise ValueError(
                    "file_name_characters may not be altered once established."
                )
            self.file_name_characters = saved["fileNameCharacters"]
            self.max_size = saved["maxSize"]
            self.reduce_size = saved["reduceSize"]
            self.current_size = saved["currentSize"]
            # The table was built with the caller's guess at the name width;
            # rebuild it now that the stored width is known, or its rows
            # would be the wrong size.
            self.binary_table = self._make_binary_table()

        if max_size is not None:
            need_to_set = True
            self.max_size = int(max_size)
        if reduce_size is not None:
            self.reduce_size = int(reduce_size)

        if need_to_set:
            self.set_properties(self.max_size, self.reduce_size, self.current_size)

    # -- the index file ---------------------------------------------------

    def _info_file_name(self):
        return os.path.join(self.directory_name, self.CACHE_INFO_FILE_NAME)

    def _make_binary_table(self):
        return BinaryTable(
            Fileobj(fullpathfilename=self._info_file_name()),
            ["char", "double", "uint64"],  # name, last-accessed time, size
            [self.file_name_characters * 1, 8, 8],
            [self.file_name_characters, 1, 1],
            self.HEADER_SIZE,
        )

    def _table(self):
        if self.binary_table is None:
            self.binary_table = self._make_binary_table()
        return self.binary_table

    def set_properties(self, max_size=None, reduce_size=None, current_size=0):
        """Set the cache's size limits and its recorded current size."""
        max_size = self.DEFAULT_MAX_SIZE if max_size is None else int(max_size)
        reduce_size = (
            self.DEFAULT_REDUCE_SIZE if reduce_size is None else int(reduce_size)
        )
        current_size = int(current_size)

        if max_size < 1000:
            raise ValueError("max_size must be at least 1000.")
        if reduce_size < 800:
            raise ValueError("reduce_size must be at least 800.")
        if reduce_size >= max_size:
            raise ValueError("reduce_size must be less than max_size.")
        if current_size < 0:
            raise ValueError("current_size must be non-negative.")

        table = self._table()
        if os.path.isfile(self._info_file_name()):
            # Keep whatever name width the file was created with: changing it
            # would reinterpret every existing row.
            stored = table.read_header()[0:2]
            name_characters = bytes(stored)
        else:
            name_characters = struct.pack("<H", self.file_name_characters)

        self.max_size = max_size
        self.reduce_size = reduce_size
        self.current_size = current_size
        table.write_header(
            name_characters + struct.pack("<QQQ", max_size, reduce_size, current_size)
        )
        return self

    def get_properties(self):
        """Read the cache's settings back from ``.fileCacheInfo``."""
        header = self._table().read_header()
        if len(header) < self.HEADER_SIZE:
            raise OSError(
                f'The cache information file "{self._info_file_name()}" is '
                f"missing or truncated ({len(header)} of "
                f"{self.HEADER_SIZE} header bytes)."
            )
        return {
            "fileNameCharacters": struct.unpack("<H", header[0:2])[0],
            "maxSize": struct.unpack("<Q", header[2:10])[0],
            "reduceSize": struct.unpack("<Q", header[10:18])[0],
            "currentSize": struct.unpack("<Q", header[18:26])[0],
        }

    # -- contents ---------------------------------------------------------

    def is_file(self, file_name_in_cache):
        """Is a file with this name in the cache?"""
        row, _ = self._table().find_row(1, file_name_in_cache)
        return row > 0

    def file_list(self, use_catalog=True):
        """Return ``(names, sizes, last_access)`` for the cached files.

        By default this reads the index. Pass ``use_catalog=False`` to list
        the directory itself instead, in which case last-access times are
        unknown and come back as NaN.
        """
        if use_catalog:
            table = self._table()
            lock_fid, key = table.get_lock()
            try:
                rows, _, _ = table.get_size()
                if rows == 0:
                    return [], [], []
                names = table.read_row(None, 1)
                last_access = table.read_row(None, 2)
                sizes = table.read_row(None, 3)
            finally:
                table.release_lock(lock_fid, key)
            return names, sizes, last_access

        names = []
        sizes = []
        for entry in os.scandir(self.directory_name):
            if entry.is_dir() or entry.name.startswith("."):
                continue
            names.append(entry.name)
            sizes.append(entry.stat().st_size)
        return names, sizes, [math.nan] * len(names)

    def full_path(self, file_name_in_cache):
        """The path a cached file has (or would have) inside the cache."""
        return os.path.join(self.directory_name, file_name_in_cache)

    def add_file(self, full_path_file_name, file_name_in_cache=None, copy=False):
        """Move (or copy) a file into the cache, evicting others if needed.

        The source file should be outside the cache directory. It is moved
        by default; pass ``copy=True`` to leave the original in place.
        """
        if not os.path.isfile(full_path_file_name):
            raise FileNotFoundError(f'There is no file at "{full_path_file_name}".')

        if not file_name_in_cache:
            file_name_in_cache = os.path.basename(full_path_file_name)

        if len(file_name_in_cache) != self.file_name_characters:
            raise ValueError(
                f"FileName has wrong number of characters (expected "
                f"{self.file_name_characters})."
            )

        table = self._table()
        lock_fid, key = table.get_lock()
        try:
            row, _ = table.find_row(1, file_name_in_cache)
            if row:
                raise ValueError(
                    f"There is already a file with name {file_name_in_cache} "
                    f"in the cache."
                )

            size = os.path.getsize(full_path_file_name)
            self.resize_and_add(size, file_name_in_cache)  # now it is in the index
            destination = self.full_path(file_name_in_cache)
            if copy:
                shutil.copyfile(full_path_file_name, destination)
            else:
                shutil.move(full_path_file_name, destination)
        finally:
            table.release_lock(lock_fid, key)

    def remove_file(self, file_name_in_cache):
        """Remove one file from the cache and from the index."""
        table = self._table()
        lock_fid, key = table.get_lock()
        try:
            row, _ = table.find_row(1, file_name_in_cache)
            if not row:
                raise ValueError(
                    f"File {file_name_in_cache} is not in file cache manifest."
                )
            properties = self.get_properties()
            size_here = table.read_row(row, 3)
            self.set_properties(
                self.max_size,
                self.reduce_size,
                max(0, properties["currentSize"] - size_here),
            )
            table.delete_row(row)
            path = self.full_path(file_name_in_cache)
            if os.path.exists(path):
                os.remove(path)
        finally:
            table.release_lock(lock_fid, key)

    def clear(self):
        """Remove every file from the cache. Use with caution."""
        table = self._table()
        lock_fid, key = table.get_lock()
        try:
            names, _, _ = self.file_list(False)
            table.write_table([])
            self.set_properties(self.max_size, self.reduce_size, 0)
            for name in names:
                path = self.full_path(name)
                if os.path.exists(path):
                    os.remove(path)
        finally:
            table.release_lock(lock_fid, key)

    def touch(self, file_name):
        """Record that a cached file has just been used.

        Returns True if the file was in the index. The last-access time is
        what eviction sorts on, so a reader that does not touch what it
        reads will see its files evicted as though never used.
        """
        table = self._table()
        lock_fid, key = table.get_lock()
        try:
            row, _ = table.find_row(1, file_name)
            if not row:
                return False
            table.write_entry(row, 2, datenum())
            return True
        finally:
            table.release_lock(lock_fid, key)

    def resize_and_add(self, new_file_size, new_file_name):
        """Record new files in the index, evicting old ones to make room.

        If the additions would push the cache past ``max_size``, the
        least recently accessed files are deleted until the total would sit
        under ``reduce_size``. This updates the index only -- ``add_file``
        puts the file itself in place.
        """
        if isinstance(new_file_name, str):
            new_file_name = [new_file_name]
        else:
            new_file_name = list(new_file_name)
        if isinstance(new_file_size, (int, float)):
            new_file_size = [int(new_file_size)]
        else:
            new_file_size = [int(size) for size in new_file_size]

        if len(new_file_size) != len(new_file_name):
            raise ValueError(
                "new_file_size and new_file_name must have the same number of entries."
            )
        if sum(new_file_size) > self.max_size:
            raise ValueError(
                "New files to be added exceed cache allowed size by themselves."
            )

        table = self._table()
        lock_fid, key = table.get_lock()
        try:
            properties = self.get_properties()
            new_total_size = properties["currentSize"] + sum(new_file_size)

            if new_total_size > self.max_size:
                names, sizes, last_access = self.file_list(True)
                # Most recently accessed first, so the tail is what goes.
                order = sorted(
                    range(len(names)), key=lambda i: last_access[i], reverse=True
                )
                running = sum(new_file_size)
                cutoff = len(order)
                for position, index in enumerate(order):
                    running += sizes[index]
                    if running > self.reduce_size:
                        cutoff = position
                        break

                for index in order[cutoff:]:
                    path = self.full_path(names[index])
                    if os.path.exists(path):
                        os.remove(path)

                kept = order[:cutoff]
                rows = [(names[i], last_access[i], sizes[i]) for i in kept] + [
                    (name, datenum(), size)
                    for name, size in zip(new_file_name, new_file_size)
                ]
                rows.sort(key=lambda row: row[0])  # keep the index sorted by name
                table.write_table([list(row) for row in rows])
                self.set_properties(
                    self.max_size, self.reduce_size, sum(row[2] for row in rows)
                )
            else:
                for name, size in zip(new_file_name, new_file_size):
                    row, insert_spot = table.find_row(1, name, sorted=True)
                    if row:
                        raise ValueError(
                            f"There is already a file with name {name} in the cache."
                        )
                    table.insert_row(insert_spot, [name, datenum(), size])
                self.set_properties(self.max_size, self.reduce_size, new_total_size)
        finally:
            table.release_lock(lock_fid, key)


def fileid_value(fid_or_fileobj):
    """
    Returns the file identifier from a raw FID or a Fileobj object.
    """
    if isinstance(fid_or_fileobj, Fileobj):
        return fid_or_fileobj.fid
    else:
        return fid_or_fileobj


def filesep_conversion(filestring, orig_filesep, new_filesep):
    """
    Converts file separators in a path string.
    """
    return filestring.replace(orig_filesep, new_filesep)


def is_filepath_root(filepath):
    """
    Determines if a file path is at the root or not.
    """
    return os.path.isabs(filepath)


def full_filename(filename):
    """
    Returns the full path file name of a file.
    """
    return os.path.abspath(filename)


def is_url(input_string):
    """
    Checks if a string is a URL.
    """
    try:
        result = urlparse(input_string)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def read_lines(file_path):
    """
    Reads lines of a file as a list of strings.
    """
    with open(file_path, "r") as f:
        lines = f.readlines()
    # Remove trailing newline characters
    return [line.rstrip("\n") for line in lines]


class ReadOnlyFileobj(Fileobj):
    def __init__(self, fullpathfilename="", machineformat="n"):
        super().__init__(
            fullpathfilename=fullpathfilename,
            permission="r",
            machineformat=machineformat,
        )

    def fopen(self, permission=None, machineformat=None, filename=None):
        if permission and "r" not in permission:
            raise ValueError("Read-only file must be opened with 'r' permission.")
        return super().fopen(
            permission="r", machineformat=machineformat, filename=filename
        )


def str_to_text(filename, s):
    """
    Writes a string to a text file.
    """
    with open(filename, "w") as f:
        f.write(s)


def string_to_filestring(s):
    """
    Edits a string so it is suitable for use as part of a filename.
    """
    return re.sub(r"[^a-zA-Z0-9]", "_", s)


def text_to_cellstr(filename):
    """
    Reads a text file and imports each line as an entry in a list of strings.
    This is an alias for read_lines.
    """
    return read_lines(filename)
