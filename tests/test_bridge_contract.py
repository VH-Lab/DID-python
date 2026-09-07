"""Rules about the bridge YAML that need no DID-matlab checkout.

`bin/check_bridge_coverage.py` compares the bridge against DID-matlab, so it
can only run where DID-matlab is checked out -- in CI, one job. The rules here
are about the bridge's *internal* consistency (one entry per thing, a status
vocabulary that means something, documentation that matches the mechanism), and
need nothing but the YAML and the Markdown in this repository.

That is deliberate. A check that lives only in the MATLAB-dependent job is one
`if` away from never running, and a bridge test that skips itself when the
MATLAB tree is missing would go green in every job that lacks it -- reporting
"passed" for a comparison it did not make. These run in every `test` matrix
job, unconditionally, and fail rather than skip.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
BRIDGE_FILES = sorted(REPO_ROOT.glob("src/did/did_matlab_python_bridge*.yaml"))
INSTRUCTIONS = REPO_ROOT / "PORTING_INSTRUCTIONS.md"


def _load_checker():
    """Import bin/check_bridge_coverage.py, which is a script, not a module.

    Importing it rather than copying its constants is the point: the tests
    below assert that the documentation matches *the tuple the checker
    actually enforces*, and a copy would let the two drift while every test
    still passed.
    """
    path = REPO_ROOT / "bin" / "check_bridge_coverage.py"
    spec = importlib.util.spec_from_file_location("check_bridge_coverage", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


checker = _load_checker()


def _bridge_documents() -> list[tuple[Path, dict[str, Any]]]:
    return [
        (p, yaml.safe_load(p.read_text(encoding="utf-8")) or {}) for p in BRIDGE_FILES
    ]


def _tracked(data: dict[str, Any]):
    for section in ("classes", "functions"):
        for entry in data.get(section) or []:
            if isinstance(entry, dict):
                yield entry


def test_there_are_bridge_files_to_check():
    """Every assertion below is vacuously true over an empty file list, so a
    rename or a moved directory would turn this whole module green while
    checking nothing."""
    assert BRIDGE_FILES, "no did_matlab_python_bridge*.yaml found under src/did"


# ---------------------------------------------------------------------------
# One thing, one entry
# ---------------------------------------------------------------------------


def duplicated_entries(documents) -> list[str]:
    """Bridge entries that record one MATLAB file twice under one name.

    Returns a description per offending (name, matlab_path); empty when clean.
    """
    by_key: dict[tuple[str, str], list[str]] = {}
    for source, data in documents:
        for entry in _tracked(data):
            path, name = entry.get("matlab_path"), entry.get("name")
            if not isinstance(path, str) or not isinstance(name, str):
                continue
            by_key.setdefault((name, path), []).append(source.name)

    return [
        f"{name} ({path}) -- {len(where)} entries in: {', '.join(where)}"
        for (name, path), where in sorted(by_key.items())
        if len(where) > 1
    ]


class TestOneMatlabFileIsRecordedOnce:
    """A MATLAB file gets ONE bridge entry under one name.

    Nothing checked this. Two entries for one file do not fail the `file`
    coverage check -- that check asks whether a MATLAB file is *recorded*, and
    twice is recorded -- so a stale copy sits there disagreeing with a real one
    until somebody reads both, and whichever a reader hits first is the answer
    they get. The bridge is the answer to "is this ported?", so two answers are
    worse than none. The sibling repository NDI-python had accumulated eleven
    such pairs before anyone looked, four of which disagreed with each other
    *and* with the Python tree.

    DID-python's bridge is clean today. This guard is here so it stays clean:
    the failure is invisible to every other check by construction, so it is
    cheap to introduce and expensive to notice.

    WHY THE KEY IS name AND path, NOT PATH ALONE. Entries legitimately share a
    `matlab_path` and a `python_path` -- twenty-four function entries resolve
    to `did/file.py`, and those are twenty-four different functions in one
    module. A guard keyed on path alone would call all of them errors, which is
    the fastest way to get a checker switched off. A duplicate is two entries
    claiming to be the SAME thing: same name, same file.
    """

    def test_no_matlab_file_is_recorded_twice_under_one_name(self):
        offenders = duplicated_entries(_bridge_documents())
        assert not offenders, (
            "these MATLAB files are recorded twice under one name:\n  "
            + "\n  ".join(offenders)
            + "\n\nKeep the entry that matches the Python tree today and delete "
            "the other."
        )

    def test_names_are_unique_across_all_bridge_files(self):
        """`name` is a key, not a label.

        `bin/check_bridge_coverage.py` builds `by_name = {e["name"]: e ...}` to
        resolve `inherits_matlab`, so a repeated name silently shadows an entry
        there and the inherited-member check quietly consults the wrong class.
        A duplicate name is also how the pair in the test above gets created in
        the first place.
        """
        seen: dict[str, list[str]] = {}
        for source, data in _bridge_documents():
            for entry in _tracked(data):
                name = entry.get("name")
                if isinstance(name, str):
                    seen.setdefault(name, []).append(source.name)
        offenders = [
            f"{name}: {len(where)} entries in {', '.join(where)}"
            for name, where in sorted(seen.items())
            if len(where) > 1
        ]
        assert not offenders, "bridge entry names must be unique:\n  " + "\n  ".join(
            offenders
        )


class TestTheDuplicateGuardWouldActuallyCatchOne:
    """The guard above passes on a clean tree, which is also what a guard that
    checks nothing does. These build the offending shapes by hand.
    """

    @staticmethod
    def _doc(entries):
        return [(Path("did_matlab_python_bridge.yaml"), {"classes": entries})]

    def test_a_clean_pair_of_different_entries_is_not_flagged(self):
        assert (
            duplicated_entries(
                self._doc(
                    [
                        {"name": "alpha", "matlab_path": "+did/alpha.m"},
                        {"name": "beta", "matlab_path": "+did/beta.m"},
                    ]
                )
            )
            == []
        )

    def test_two_entries_for_one_name_and_path_are_flagged(self):
        offenders = duplicated_entries(
            self._doc(
                [
                    {"name": "alpha", "matlab_path": "+did/alpha.m"},
                    {"name": "alpha", "matlab_path": "+did/alpha.m"},
                ]
            )
        )
        assert len(offenders) == 1 and "alpha" in offenders[0]

    def test_a_duplicate_split_across_two_files_is_flagged(self):
        offenders = duplicated_entries(
            [
                (Path("a.yaml"), {"classes": [{"name": "a", "matlab_path": "x.m"}]}),
                (Path("b.yaml"), {"functions": [{"name": "a", "matlab_path": "x.m"}]}),
            ]
        )
        assert len(offenders) == 1
        assert "a.yaml" in offenders[0] and "b.yaml" in offenders[0]

    def test_different_names_sharing_one_matlab_file_are_not_flagged(self):
        """Methods recorded off one classdef, and the twenty-four function
        entries under did/file.py, are not duplicates. Flagging them is how a
        checker gets ignored."""
        assert (
            duplicated_entries(
                self._doc(
                    [
                        {"name": "readlines", "matlab_path": "+did/+file/util.m"},
                        {"name": "str2text", "matlab_path": "+did/+file/util.m"},
                    ]
                )
            )
            == []
        )


# ---------------------------------------------------------------------------
# The status vocabulary, and the documentation of it
# ---------------------------------------------------------------------------


def _documented_status_table() -> list[tuple[str, str]]:
    """Rows of the status table in PORTING_INSTRUCTIONS.md § Status vocabulary.

    Returns (status literal, allowed-in-not_tracked) for each row whose first
    cell is a single backquoted value. The `*(absent)*` row -- the implicit
    `ported` default -- has no literal and is checked separately.
    """
    text = INSTRUCTIONS.read_text(encoding="utf-8")
    start = text.index("## Status vocabulary")
    section = text[start : text.index("\n## ", start + 1)]

    # The section holds a second table (the `not_tracked` field reference), so
    # anchor on the header row rather than scanning for anything table-shaped.
    lines = section.splitlines()
    header = "| `status` | Meaning | Allowed in `not_tracked`? |"
    assert header in lines, (
        "the status table's header row in PORTING_INSTRUCTIONS.md is not "
        f"{header!r} any more, so this test is reading the wrong table. "
        "Update the anchor deliberately."
    )

    rows = []
    for line in lines[lines.index(header) + 1 :]:
        if not line.strip().startswith("|"):
            break
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 3:
            continue
        match = re.fullmatch(r"`([a-z_]+)`", cells[0])
        if match:
            rows.append((match.group(1), cells[2].lower()))
    return rows


class TestTheDriftAllowlistNamesRealEntries:
    """`DRIFT_ALLOWLIST` exempts entries from the gating drift check by `name`.

    A typo in it exempts nothing and reads as though it exempts something --
    the same inert-entry failure the `not_tracked` list had, in a place where
    the consequence is a gate that quietly does not cover what someone thought
    they had parked. Empty today (nothing here was drifted when the gate went
    on, per NDI-python issue #211), so this guards the list's future use.

    Whether an allowlisted entry is STILL drifting needs DID-matlab and is
    checked by `check_drift`, which reports a stale entry so the ratchet
    tightens. This only asks that the names exist.
    """

    def test_every_allowlisted_name_is_a_bridge_entry(self):
        known = {
            entry.get("name")
            for _, data in _bridge_documents()
            for entry in _tracked(data)
        }
        unknown = [n for n in checker.DRIFT_ALLOWLIST if n not in known]
        assert not unknown, (
            "DRIFT_ALLOWLIST names entries that do not exist: "
            f"{unknown}\nIt exempts by `name`, so a typo exempts nothing while "
            "looking like it does."
        )

    def test_the_allowlist_has_no_duplicates(self):
        seen = list(checker.DRIFT_ALLOWLIST)
        assert len(seen) == len(set(seen)), f"duplicate names: {seen}"


def _documented_retired_statuses() -> set[str]:
    """Names in PORTING_INSTRUCTIONS.md § Retired status names.

    Anchored on the header row, like the vocabulary table above, so the two
    tables in this section cannot be confused for each other.
    """
    text = INSTRUCTIONS.read_text(encoding="utf-8")
    start = text.index("### Retired status names")
    section = text[start : text.index("\n### ", start + 1)]
    lines = section.splitlines()
    header = "| Retired name | Write instead | Why |"
    assert header in lines, (
        "the retired-names table header in PORTING_INSTRUCTIONS.md is not "
        f"{header!r} any more, so this test is reading the wrong table."
    )
    names = set()
    for line in lines[lines.index(header) + 1 :]:
        if not line.strip().startswith("|"):
            break
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        match = re.fullmatch(r"`([a-z_]+)`", cells[0]) if cells else None
        if match:
            names.add(match.group(1))
    return names


class TestDocumentationMatchesTheMechanism:
    """PORTING_INSTRUCTIONS.md § Status vocabulary is normative, and
    `bin/check_bridge_coverage.py` is what actually enforces it. Nothing keeps
    a normative document honest on its own: NDI-python's bridge spec came to
    contradict itself precisely because one rule was written in several places
    and only one copy got updated. These tests bind the prose to the tuple, so
    a value cannot be added to either without the other.
    """

    def test_the_status_section_exists(self):
        text = INSTRUCTIONS.read_text(encoding="utf-8")
        assert "## Status vocabulary" in text, (
            "PORTING_INSTRUCTIONS.md no longer has a 'Status vocabulary' section. "
            "It is the single normative definition of these values; if it moved, "
            "point this test at the new home rather than deleting the binding."
        )

    def test_documented_statuses_are_exactly_the_enforced_ones(self):
        documented = {status for status, _ in _documented_status_table()}
        assert documented == set(checker.STATUSES), (
            "PORTING_INSTRUCTIONS.md and check_bridge_coverage.py disagree about "
            f"the status vocabulary.\n  documented: {sorted(documented)}\n  "
            f"enforced:   {sorted(checker.STATUSES)}"
        )

    def test_documented_not_tracked_statuses_match(self):
        documented = {
            status
            for status, allowed in _documented_status_table()
            if allowed.startswith("yes")
        }
        assert documented == set(checker.NOT_TRACKED_STATUSES), (
            "the 'Allowed in not_tracked?' column disagrees with "
            f"NOT_TRACKED_STATUSES.\n  documented: {sorted(documented)}\n  "
            f"enforced:   {sorted(checker.NOT_TRACKED_STATUSES)}"
        )

    def test_documented_retired_names_are_exactly_the_rejected_ones(self):
        """The rename is only safe if every place that names the value moves
        together. NDR-python issue #21 lists four such places per repo, and a
        half-finished rename is strictly worse than either name -- so bind the
        table to the dict the checker actually rejects on."""
        documented = _documented_retired_statuses()
        assert documented == set(checker.REPLACED_STATUSES), (
            "PORTING_INSTRUCTIONS.md and check_bridge_coverage.py disagree "
            f"about retired status names.\n  documented: {sorted(documented)}\n"
            f"  rejected:   {sorted(checker.REPLACED_STATUSES)}"
        )

    def test_no_name_is_both_current_and_retired(self):
        """A name in both tuples would make the checker reject a value its own
        vocabulary allows, in whichever order the branches happen to run."""
        overlap = set(checker.STATUSES) & set(checker.REPLACED_STATUSES)
        assert not overlap, f"status names both current and retired: {sorted(overlap)}"

    def test_ported_is_documented_as_the_absent_default(self):
        """`ported` is spelled by leaving `status` out, so it is the one value
        with no literal to compare -- and the one a reader is most likely to
        write out anyway. The table must keep saying so."""
        text = INSTRUCTIONS.read_text(encoding="utf-8")
        section = text[text.index("## Status vocabulary") :]
        section = section[: section.index("\n## ", 1)]
        assert "*(absent)*" in section and "ported" in section
        assert "ported" not in checker.STATUSES, (
            "`ported` became an explicit status. If that is intended, the "
            "default's spelling has changed and the table needs rewriting; the "
            "reason it was implicit is that an absent status would otherwise be "
            "ambiguous between 'ported' and 'not filled in'."
        )


class TestStatusValuesInUse:
    @pytest.mark.parametrize("source", BRIDGE_FILES, ids=lambda p: p.name)
    def test_tracked_statuses_are_in_the_vocabulary(self, source: Path):
        data = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
        offenders = [
            f"{entry.get('name')}: {entry['status']!r}"
            for entry in _tracked(data)
            if entry.get("status") is not None
            and entry["status"] not in checker.STATUSES
        ]
        assert not offenders, (
            f"{source.name}: statuses outside the documented vocabulary "
            f"{list(checker.STATUSES)}:\n  " + "\n  ".join(offenders)
        )

    @pytest.mark.parametrize("source", BRIDGE_FILES, ids=lambda p: p.name)
    def test_not_tracked_entries_carry_a_status_and_a_reason(self, source: Path):
        data = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
        offenders = []
        for entry in data.get("not_tracked") or []:
            name = entry.get("name", "<unnamed>")
            if entry.get("status") not in checker.NOT_TRACKED_STATUSES:
                offenders.append(f"{name}: status {entry.get('status')!r}")
            elif len((entry.get("decision_log") or "").split()) < 5:
                offenders.append(f"{name}: status with no decision_log")
        assert not offenders, f"{source.name}:\n  " + "\n  ".join(offenders)

    @pytest.mark.parametrize("source", BRIDGE_FILES, ids=lambda p: p.name)
    def test_no_entry_uses_a_retired_status_name(self, source: Path):
        """Catches a rename that changed the constants but missed an entry.

        `test_tracked_statuses_are_in_the_vocabulary` would also fail on this,
        but with "outside the documented vocabulary" -- true, and unhelpful.
        This one names the replacement.
        """
        data = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
        entries = list(_tracked(data)) + list(data.get("not_tracked") or [])
        offenders = [
            f"{entry.get('name')}: {entry['status']!r} -> use "
            f"{checker.REPLACED_STATUSES[entry['status']]}"
            for entry in entries
            if entry.get("status") in checker.REPLACED_STATUSES
        ]
        assert not offenders, f"{source.name}: retired status names:\n  " + "\n  ".join(
            offenders
        )

    @pytest.mark.parametrize("source", BRIDGE_FILES, ids=lambda p: p.name)
    def test_no_status_is_hidden_as_prose_in_a_path_field(self, source: Path):
        """`python_path: "(not separately implemented)"` was how this repo said
        `ported_differently` before the field existed, and
        `python_path: "(not applicable)"` was how it said `porting_deferred` --
        the same shape for opposite claims, and the checker skipped both on the
        leading `(`, which disabled every check keyed on that path. Paths name
        files. Statuses go in `status`.
        """
        data = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
        offenders = [
            f"{entry.get('name')}.{field} = {entry[field]!r}"
            for entry in _tracked(data)
            for field in ("python_path", "python_class", "python_name")
            if isinstance(entry.get(field), str)
            and entry[field].strip().startswith("(")
        ]
        assert not offenders, (
            f"{source.name}: prose in a field that names code:\n  "
            + "\n  ".join(offenders)
            + "\n\nUse `status:` -- see PORTING_INSTRUCTIONS.md, "
            "'Statuses are never prose in a path field'."
        )
