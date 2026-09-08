"""makeArtifact symmetry test: conformance vectors for the NAME_<i> parse rule.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+makeArtifacts/+common/seriesMemberNames.m.

Not a file format: a RULE, which is why it sits beside path_agreement rather
than in file/. ``Document.series_member_of`` decides whether a filename names
a series member, and every other resolution rests on it -- a database looks
for bytes because of it. The rule is implemented once per language, from
prose, with nothing shared to check against.

Getting it wrong is not a crash. A rule that is too permissive turns every
NAME_<n> in a document into a member of some series and sends resolution
after files that do not exist; one that is too strict makes real members
unreachable. Both are quiet.

The vectors are asserted against the live implementation before being written
out, so this file cannot claim something DID-python does not do.

THE PARSE. Both languages use a strict all-digits check on the trailing part
after the last underscore. MATLAB used to parse with str2num, which EVALUATES
its argument, so 'chunkdata.bin_1+1' would parse as member 2 there and
'_pi', '_i', '_-1' all came back as numbers. Tightened in
DID-matlab#199 / DID-python#69 so the two languages agree at the parse rather
than at a downstream check. Every vector here is an input both languages now
resolve the same way.

Artifacts are written to:
    <tempdir>/DID/symmetryTest/pythonArtifacts/common/seriesMemberNames/testSeriesMemberNamesArtifacts/
"""

import json
import os
import shutil

from did.document import Document
from tests.symmetry.conftest import PYTHON_ARTIFACTS

DOCUMENT_CLASS = "demoSeries"
# demoSeries declares chunkdata.bin as a file SERIES and plainfile.ext as an
# ordinary file. Both are in the file list; only one is a series, which is
# the distinction the rule turns on.
SERIES_NAME = "chunkdata.bin"
ORDINARY_FILE_NAME = "plainfile.ext"

ARTIFACT_DIR = os.path.join(
    PYTHON_ARTIFACTS,
    "common",
    "seriesMemberNames",
    "testSeriesMemberNamesArtifacts",
)

# (name, expected stem ("" for a miss), expected index (None for a miss), why)
VECTORS = [
    ("chunkdata.bin_1", SERIES_NAME, 1, "the ordinary case"),
    (
        "chunkdata.bin_12",
        SERIES_NAME,
        12,
        "multi-digit, so a single-character parse fails here",
    ),
    ("chunkdata.bin_007", SERIES_NAME, 7, "leading zeros are not significant"),
    (
        "CHUNKDATA.BIN_3",
        SERIES_NAME,
        3,
        (
            "matching is case-insensitive AND the DECLARED spelling comes back, "
            "not the caller's: everything downstream looks the stem up again "
            "where the declared spelling is what is stored"
        ),
    ),
    (
        "chunkdata.bin_0",
        SERIES_NAME,
        0,
        (
            "parses; open_doc is what rejects an index below 1, as NoSuchMember "
            "rather than as a damaged manifest (DID-python#80)"
        ),
    ),
    (
        "chunkdata.bin_1_2",
        "",
        None,
        (
            'the LAST underscore splits, so the candidate stem is "chunkdata.bin_1", '
            "which is not declared. A first-underscore or greedy match would "
            "wrongly resolve this"
        ),
    ),
    (
        "plainfile.ext_1",
        "",
        None,
        (
            "declared as an ordinary FILE, not a series. Without this check every "
            "numbered filename becomes a member"
        ),
    ),
    ("chunkdata.bin", "", None, "the series name is the manifest, never a member"),
    ("chunkdata.bin_", "", None, "nothing after the underscore is not an index"),
    ("nosuchseries_1", "", None, "an undeclared stem"),
    ("_1", "", None, "an empty candidate stem"),
    ("", "", None, "the empty name is a miss, not an error"),
    (
        "chunkdata.bin_-1",
        "",
        None,
        (
            "fails the parse itself in both languages: MATLAB's str2num used to "
            "accept the leading minus, but the strict all-digits check does not "
            "(DID-matlab#199 / DID-python#69). Slots are one-based, so nothing "
            "can mint this name -- see DID-python#80."
        ),
    ),
]


class TestSeriesMemberNamesArtifacts:
    def test_series_member_names_artifacts(self):
        if os.path.isdir(ARTIFACT_DIR):
            shutil.rmtree(ARTIFACT_DIR)
        os.makedirs(ARTIFACT_DIR, exist_ok=True)

        doc = Document(DOCUMENT_CLASS, **{"demoSeries.value": 1})

        cases = []
        for name, expected_stem, expected_index, why in VECTORS:
            stem, index = doc.series_member_of(name)

            # Assert against the live rule BEFORE recording it.
            assert stem == expected_stem, f'stem for "{name}"'
            assert index == expected_index, f'index for "{name}"'

            cases.append(
                {
                    "name": name,
                    "isMember": bool(stem),
                    "stem": stem,
                    # null, not 0: a miss has no index, and 0 is itself a
                    # legitimate parsed value in this table.
                    "index": index,
                    "why": why,
                }
            )

        manifest = {
            "documentClass": DOCUMENT_CLASS,
            "declaredSeries": [SERIES_NAME],
            "declaredOrdinaryFiles": [ORDINARY_FILE_NAME],
            "rule": (
                "split on the LAST underscore; the tail must parse as a "
                "number; the head must be a DECLARED series name, matched "
                "case-insensitively; the declared spelling is what comes back"
            ),
            "validationLivesInTheCaller": (
                "series_member_of parses only. An index below 1 is rejected "
                "by SQLiteDB._open_series_member as NoSuchMember."
            ),
            "indexParse": "str.isdigit (strict, non-negative)",
            "cases": cases,
        }
        with open(os.path.join(ARTIFACT_DIR, "manifest.json"), "w") as handle:
            json.dump(manifest, handle, indent=2)

        # The declarations the vectors rest on, asserted rather than assumed:
        # if demoSeries ever stopped declaring these, every miss above would
        # still "pass" for the wrong reason.
        assert doc.is_file_series(
            SERIES_NAME
        ), "precondition: chunkdata.bin must be a declared series"
        assert not doc.is_file_series(
            ORDINARY_FILE_NAME
        ), "precondition: plainfile.ext must NOT be a declared series"
