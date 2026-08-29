"""did.file.BinaryTable -- the typed binary table MATLAB's fileCache is built on.

These tests pin the on-disk layout, not just the round trip. A round trip
would pass just as happily if both ends agreed on a format MATLAB cannot
read, and the whole point of this class is that MATLAB can.
"""

import math
import os
import struct

import pytest

from did.file import BinaryTable, Fileobj

# MATLAB: did.file.binaryTable(fileobj, {'char','double','uint64'},
#                              [33 8 8], [33 1 1], 2+8+8+8)
NAME_CHARACTERS = 33
RECORD_TYPE = ["char", "double", "uint64"]
RECORD_SIZE = [NAME_CHARACTERS, 8, 8]
ELEMENTS_PER_COLUMN = [NAME_CHARACTERS, 1, 1]
HEADER_SIZE = 2 + 8 + 8 + 8
ROW_SIZE = NAME_CHARACTERS + 8 + 8


def make_table(tmp_path, name="table.bin"):
    return BinaryTable(
        Fileobj(fullpathfilename=str(tmp_path / name)),
        RECORD_TYPE,
        RECORD_SIZE,
        ELEMENTS_PER_COLUMN,
        HEADER_SIZE,
    )


def name_of(index):
    return f"{index:033d}"


class TestLayout:
    def test_a_column_whose_size_contradicts_its_type_is_rejected(self, tmp_path):
        # 33 doubles do not fit in 33 bytes. Accepting this would put every
        # row boundary in the wrong place, silently.
        with pytest.raises(ValueError, match="occupy"):
            BinaryTable(
                Fileobj(fullpathfilename=str(tmp_path / "t.bin")),
                ["double"],
                [33],
                [33],
                0,
            )

    def test_an_unknown_record_type_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="Unsupported record type"):
            BinaryTable(
                Fileobj(fullpathfilename=str(tmp_path / "t.bin")),
                ["complex"],
                [16],
                [1],
                0,
            )

    def test_mismatched_column_descriptions_are_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="one per column"):
            BinaryTable(
                Fileobj(fullpathfilename=str(tmp_path / "t.bin")),
                ["char", "double"],
                [33],
                [33, 1],
                0,
            )

    def test_row_size_is_the_sum_of_the_column_sizes(self, tmp_path):
        assert make_table(tmp_path).row_size() == ROW_SIZE

    def test_the_lock_file_is_the_one_matlab_checks_out(self, tmp_path):
        # MATLAB's binaryTable.lockFileName() is [fullpathfilename '-lock'].
        # If Python used a different name the two would not exclude each
        # other at all while appearing to lock.
        table = make_table(tmp_path)
        assert table.lock_file_name() == str(tmp_path / "table.bin") + "-lock"
        assert table.temp_file_name() == str(tmp_path / "table.bin") + "-temp"


class TestOnDiskFormat:
    def test_rows_are_written_in_matlabs_layout(self, tmp_path):
        table = make_table(tmp_path)
        table.write_header(
            struct.pack("<H", NAME_CHARACTERS) + struct.pack("<QQQ", 1000, 800, 0)
        )
        table.insert_row(0, [name_of(7), 738000.5, 4096])

        raw = (tmp_path / "table.bin").read_bytes()
        assert len(raw) == HEADER_SIZE + ROW_SIZE

        # Header: uint16 then three uint64, little-endian.
        assert struct.unpack("<H", raw[0:2])[0] == NAME_CHARACTERS
        assert struct.unpack("<QQQ", raw[2:26]) == (1000, 800, 0)

        # Row: 33 bytes of characters, then a double, then a uint64.
        row = raw[HEADER_SIZE:]
        assert row[0:NAME_CHARACTERS].decode("latin-1") == name_of(7)
        assert (
            struct.unpack("<d", row[NAME_CHARACTERS : NAME_CHARACTERS + 8])[0]
            == 738000.5
        )
        assert struct.unpack("<Q", row[NAME_CHARACTERS + 8 :])[0] == 4096

    def test_a_table_written_bytewise_reads_back_as_typed_values(self, tmp_path):
        # Build the file the way MATLAB's fwrite would, without going through
        # BinaryTable at all, and then read it with BinaryTable.
        path = tmp_path / "hand.bin"
        raw = struct.pack("<H", NAME_CHARACTERS) + struct.pack("<QQQ", 5000, 4000, 12)
        for index, (size, when) in enumerate([(4, 700000.25), (8, 700001.75)]):
            raw += name_of(index).encode("latin-1")
            raw += struct.pack("<d", when)
            raw += struct.pack("<Q", size)
        path.write_bytes(raw)

        table = make_table(tmp_path, "hand.bin")
        assert table.get_size()[0] == 2
        assert table.read_row(None, 1) == [name_of(0), name_of(1)]
        assert table.read_row(None, 2) == [700000.25, 700001.75]
        assert table.read_row(None, 3) == [4, 8]

    def test_a_new_file_is_padded_out_to_the_full_header(self, tmp_path):
        # A short header would put row 1 before header_size, so every offset
        # after it would be wrong.
        table = make_table(tmp_path)
        table.write_header(b"\x21\x00")
        assert os.path.getsize(tmp_path / "table.bin") == HEADER_SIZE


class TestReading:
    @pytest.fixture
    def populated(self, tmp_path):
        table = make_table(tmp_path)
        table.write_header(
            struct.pack("<H", NAME_CHARACTERS) + struct.pack("<QQQ", 9000, 8000, 0)
        )
        for index in range(4):
            table.insert_row(index, [name_of(index), 700000.0 + index, 100 * index])
        return table

    def test_a_single_row_returns_a_single_value(self, populated):
        assert populated.read_row(2, 1) == name_of(1)
        assert populated.read_row(2, 2) == 700001.0
        assert populated.read_row(2, 3) == 100

    def test_several_rows_return_a_list(self, populated):
        assert populated.read_row([1, 3], 1) == [name_of(0), name_of(2)]

    def test_all_rows_return_a_list(self, populated):
        assert populated.read_row(None, 3) == [0, 100, 200, 300]
        # math.inf is how MATLAB spells "all rows"; both work.
        assert populated.read_row(math.inf, 3) == [0, 100, 200, 300]

    def test_reading_past_the_end_raises(self, populated):
        with pytest.raises(IndexError):
            populated.read_row(5, 1)

    def test_an_out_of_range_column_raises(self, populated):
        with pytest.raises(ValueError, match="Column must be in"):
            populated.read_row(1, 4)

    def test_an_empty_table_reads_as_nothing(self, tmp_path):
        table = make_table(tmp_path)
        table.write_header(
            struct.pack("<H", NAME_CHARACTERS) + struct.pack("<QQQ", 9000, 8000, 0)
        )
        assert table.get_size()[0] == 0
        assert table.read_row(None, 1) == []


class TestWriting:
    @pytest.fixture
    def populated(self, tmp_path):
        table = make_table(tmp_path)
        table.write_header(
            struct.pack("<H", NAME_CHARACTERS) + struct.pack("<QQQ", 9000, 8000, 0)
        )
        for index in range(3):
            table.insert_row(index, [name_of(index), 700000.0 + index, 100 * index])
        return table

    def test_insert_at_the_front(self, populated):
        populated.insert_row(0, [name_of(9), 1.0, 1])
        assert populated.read_row(None, 1)[0] == name_of(9)
        assert populated.get_size()[0] == 4

    def test_insert_in_the_middle(self, populated):
        populated.insert_row(1, [name_of(9), 1.0, 1])
        assert populated.read_row(None, 1) == [
            name_of(0),
            name_of(9),
            name_of(1),
            name_of(2),
        ]

    def test_insert_past_the_end_is_refused(self, populated):
        # MATLAB permits rows+1 here and then writes the row past the data,
        # corrupting the table. Refusing is the whole difference.
        with pytest.raises(ValueError, match="Row must be in 0"):
            populated.insert_row(4, [name_of(9), 1.0, 1])

    def test_delete_row(self, populated):
        populated.delete_row(2)
        assert populated.read_row(None, 1) == [name_of(0), name_of(2)]
        assert populated.read_row(None, 3) == [0, 200]

    def test_delete_out_of_range_is_refused(self, populated):
        with pytest.raises(ValueError, match="Row must be in 1"):
            populated.delete_row(4)

    def test_write_entry_changes_one_value_and_nothing_else(self, populated):
        populated.write_entry(2, 2, 12345.5)
        assert populated.read_row(None, 2) == [700000.0, 12345.5, 700002.0]
        assert populated.read_row(None, 1) == [name_of(0), name_of(1), name_of(2)]

    def test_write_entry_rejects_the_wrong_type(self, populated):
        with pytest.raises(TypeError):
            populated.write_entry(1, 2, "text in a double column")

    def test_write_entry_rejects_the_wrong_length(self, populated):
        with pytest.raises(ValueError, match="characters"):
            populated.write_entry(1, 1, "too short")

    def test_write_table_replaces_the_rows_and_keeps_the_header(self, populated):
        populated.write_table([[name_of(5), 5.0, 55]])
        assert populated.read_row(None, 1) == [name_of(5)]
        assert struct.unpack("<QQQ", populated.read_header()[2:26]) == (9000, 8000, 0)

    def test_write_table_with_no_rows_empties_the_table(self, populated):
        populated.write_table([])
        assert populated.get_size()[0] == 0
        assert struct.unpack("<H", populated.read_header()[0:2])[0] == NAME_CHARACTERS

    def test_the_header_survives_inserts_and_deletes(self, populated):
        populated.insert_row(0, [name_of(9), 1.0, 1])
        populated.delete_row(2)
        assert struct.unpack("<QQQ", populated.read_header()[2:26]) == (9000, 8000, 0)


class TestFindRow:
    @pytest.fixture
    def sorted_table(self, tmp_path):
        table = make_table(tmp_path)
        table.write_header(
            struct.pack("<H", NAME_CHARACTERS) + struct.pack("<QQQ", 9000, 8000, 0)
        )
        for index, key in enumerate([2, 4, 6, 8]):
            table.insert_row(index, [name_of(key), float(key), key])
        return table

    def test_unsorted_search_finds_the_row(self, sorted_table):
        row, would_be = sorted_table.find_row(1, name_of(6))
        assert row == 3
        assert math.isnan(would_be)

    def test_unsorted_search_reports_zero_when_absent(self, sorted_table):
        row, _ = sorted_table.find_row(1, name_of(5))
        assert row == 0

    @pytest.mark.parametrize("key,expected", [(2, 1), (4, 2), (6, 3), (8, 4)])
    def test_sorted_search_finds_every_row(self, sorted_table, key, expected):
        row, _ = sorted_table.find_row(1, name_of(key), sorted=True)
        assert row == expected

    @pytest.mark.parametrize(
        "key,expected_would_be",
        [(1, 0), (3, 1), (5, 2), (7, 3), (9, 4)],
    )
    def test_sorted_search_says_where_a_missing_value_belongs(
        self, sorted_table, key, expected_would_be
    ):
        # would_be is the row to insert *after*, which is exactly what
        # insert_row takes -- so inserting there keeps the table sorted.
        row, would_be = sorted_table.find_row(1, name_of(key), sorted=True)
        assert row == 0
        assert would_be == expected_would_be

        sorted_table.insert_row(would_be, [name_of(key), float(key), key])
        names = sorted_table.read_row(None, 1)
        assert names == sorted(names)

    def test_searching_an_empty_table_puts_the_value_at_the_front(self, tmp_path):
        table = make_table(tmp_path)
        table.write_header(
            struct.pack("<H", NAME_CHARACTERS) + struct.pack("<QQQ", 9000, 8000, 0)
        )
        row, would_be = table.find_row(1, name_of(1), sorted=True)
        assert (row, would_be) == (0, 0)


class TestCompare:
    @pytest.mark.parametrize(
        "left,right,expected",
        [
            ("a", "b", 1),
            ("b", "a", -1),
            ("a", "a", 0),
            (1, 2, 1),
            (2, 1, -1),
            (2, 2, 0),
            (1.5, 2.5, 1),
            (["a"], ["b"], 1),  # a cell is compared by its first entry
            (b"a", "b", 1),
        ],
    )
    def test_ordering(self, left, right, expected):
        assert BinaryTable.compare(left, right) == expected

    def test_incomparable_values_raise(self):
        with pytest.raises(ValueError, match="Could not make comparison"):
            BinaryTable.compare("a", 1)
