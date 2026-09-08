"""readArtifact symmetry test: check the NAME_<i> parse rule against the
vectors the other language recorded.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+readArtifacts/+common/seriesMemberNames.m.

The rule ``Document.series_member_of`` implements is written once per
language, from prose, with no shared code to check against -- so the only
thing that can catch a drift is a table of inputs and answers passed between
them. A rule that is too permissive turns every NAME_<n> into a member and
sends resolution after files that do not exist; one that is too strict makes
real members unreachable. Both are quiet, and neither shows up in a test
that only round-trips its own language.

KNOWN DEVIATIONS are named in ``DEVIATIONS`` below rather than skipped
silently. A vector listed there is still read and still checked -- against
what THIS language does, with the difference asserted to be the one that was
agreed -- so if either side moves again the test fails rather than quietly
widening the allowance.

Skips when the artifact is absent rather than failing, so this can land in
either repository first without blocking the other. See conftest.py for the
CI-side gate that turns absence into a hard failure in the symmetry job.
"""

import json
import os

from did.document import Document
from tests.symmetry.conftest import SYMMETRY_BASE, missing_artifact

# name -> (this language's stem, this language's index, why they differ)
#
# Empty as of DID-matlab#198 / DID-python#69: MATLAB tightened its parse
# from str2num (which EVALUATES) to a strict all-digits check, matching
# Python's isdigit-based parse. The '_-1' vector that used to be a
# MATLAB-only member is now refused at the parse in both languages.
DEVIATIONS: dict[str, tuple[str, int | None, str]] = {}


def _recorded_index(case):
    """The index a case records, as None when it records none.

    MATLAB's jsonencode writes an absent index as ``[]`` and Python's writes
    ``null``, so both shapes arrive here and mean the same thing.
    """
    value = case.get("index")
    if value is None or value == []:
        return None
    return value


class TestReadSeriesMemberNames:
    def test_series_member_names_artifacts(self, source_type):
        artifact_dir = os.path.join(
            SYMMETRY_BASE,
            source_type,
            "common",
            "seriesMemberNames",
            "testSeriesMemberNamesArtifacts",
        )
        if not os.path.isdir(artifact_dir):
            missing_artifact(
                f"Artifact directory from {source_type} does not exist: {artifact_dir}"
            )
        manifest_file = os.path.join(artifact_dir, "manifest.json")
        if not os.path.isfile(manifest_file):
            missing_artifact(
                f"manifest.json not found in {source_type} artifact directory."
            )

        with open(manifest_file, "r") as handle:
            manifest = json.load(handle)

        doc = Document(manifest["documentClass"], **{"demoSeries.value": 1})

        # The declarations the vectors rest on. Without this, every "miss"
        # below would pass for the wrong reason if demoSeries stopped
        # declaring the series at all.
        for name in manifest["declaredSeries"]:
            assert doc.is_file_series(
                name
            ), f'"{name}" must be a declared series in {manifest["documentClass"]}'
        for name in manifest.get("declaredOrdinaryFiles", []):
            assert not doc.is_file_series(
                name
            ), f'"{name}" must NOT be a declared series'

        cases = manifest["cases"]
        assert cases, f"{source_type} recorded no vectors"

        checked = 0
        deviations_seen = set()
        for case in cases:
            name = case["name"]
            stem, index = doc.series_member_of(name)

            if name in DEVIATIONS:
                expected_stem, expected_index, why = DEVIATIONS[name]
                deviations_seen.add(name)
                assert (stem, index) == (expected_stem, expected_index), (
                    f'"{name}" is a KNOWN deviation whose Python answer is '
                    f"pinned as {(expected_stem, expected_index)} ({why}), but "
                    f"Python now answers {(stem, index)}. Either the rule "
                    f"changed or the deviation was resolved -- update "
                    f"DEVIATIONS and the bridge, do not widen it."
                )
                continue

            recorded_stem = case["stem"]
            recorded_index = _recorded_index(case)
            why = case.get("why", "")

            assert stem == recorded_stem, (
                f'stem for "{name}": {source_type} recorded '
                f"{recorded_stem!r}, this language answers {stem!r}. {why}"
            )
            assert index == recorded_index, (
                f'index for "{name}": {source_type} recorded '
                f"{recorded_index!r}, this language answers {index!r}. {why}"
            )
            # isMember must agree with the stem it was derived from, or the
            # artifact is internally inconsistent whatever the rule says.
            assert bool(case["isMember"]) == bool(
                recorded_stem
            ), f'"{name}": isMember disagrees with stem in the artifact'
            checked += 1

        assert checked > 0, (
            f"{source_type} contributed no non-deviation vectors, so this "
            f"compared nothing across languages"
        )

        # A deviation that has stopped being present in the other language's
        # table must not go unnoticed: it either got fixed (good, remove it
        # here) or the vector was dropped (and the allowance is now stale).
        if source_type == "matlabArtifacts":
            unseen = set(DEVIATIONS) - deviations_seen
            assert not unseen, (
                f"DEVIATIONS names {sorted(unseen)}, which {source_type} no "
                f"longer records. Re-check whether the difference still "
                f"exists and remove the entry if it does not."
            )
