"""Tests for ``did.fun.timestamp``, the datestamp every new document carries.

Ported from DID-matlab's TestTimestamp (bb29861, issue #53). A
``UTCLeapSeconds`` clock can render a ``':60'`` leap second, which the schema
validator rejects -- MATLAB parses with ``java.time.LocalDateTime.parse``, and
Python's ``_parses_as_timestamp`` with ``datetime.fromisoformat``, which both
refuse seconds above 59. The helper must clamp it to ``':59.999'``.
"""

import re

from did.document import Document
from did.fun import timestamp
from did.validate import _parses_as_timestamp


class TestTimestamp:
    def test_current_timestamp_is_schema_valid(self):
        ts = timestamp()
        assert isinstance(ts, str)
        assert _parses_as_timestamp(ts)

    def test_current_timestamp_has_matlabs_shape(self):
        # char(datetime('now','TimeZone','UTCLeapSeconds')) renders as
        # 'yyyy-MM-ddTHH:mm:ss.SSSZ'.
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", timestamp())

    def test_leap_second_is_clamped(self):
        raw = "2026-09-18T21:37:60.000Z"
        assert not _parses_as_timestamp(raw)
        ts = timestamp(raw)
        assert ts == "2026-09-18T21:37:59.999Z"
        assert _parses_as_timestamp(ts)

    def test_leap_second_without_trailing_z_is_clamped(self):
        assert timestamp("2026-09-18T21:37:60.000") == "2026-09-18T21:37:59.999"

    def test_leap_second_without_fraction_is_clamped(self):
        assert timestamp("2026-09-18T21:37:60Z") == "2026-09-18T21:37:59.999Z"

    def test_leap_second_fraction_is_replaced_not_appended(self):
        # The fraction is part of what is clamped: ':60.123' becomes ':59.999',
        # never ':59.999123'.
        assert timestamp("2026-09-18T21:37:60.123Z") == "2026-09-18T21:37:59.999Z"

    def test_normal_timestamp_is_unchanged(self):
        raw = "2026-09-18T21:37:45.123Z"
        assert timestamp(raw) == raw
        assert _parses_as_timestamp(raw)

    def test_a_60_outside_the_seconds_field_is_untouched(self):
        # Only the seconds field is a leap second; the regex is anchored on
        # 'THH:MM:' so a 60 elsewhere is not rewritten.
        raw = "2060-06-06T06:06:06.060Z"
        assert timestamp(raw) == raw


class TestDocumentDatestamp:
    def test_new_document_is_stamped_by_timestamp(self, monkeypatch):
        # did.document stamps base.datestamp through did.fun.timestamp, so the
        # leap-second clamp applies to every generated document.
        monkeypatch.setattr(
            "did.document.timestamp", lambda: timestamp("2026-09-18T21:37:60.000Z")
        )
        doc = Document("base")
        assert doc.document_properties["base"]["datestamp"] == (
            "2026-09-18T21:37:59.999Z"
        )
