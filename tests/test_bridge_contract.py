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


def _git(*args: str) -> tuple[int, str]:
    import subprocess

    r = subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        capture_output=True,
        text=True,
        check=False,  # a non-zero exit is an answer here, not an error
    )
    return r.returncode, r.stdout.strip()


def _merge_base() -> str | None:
    """The commit this branch diverged from, or None if it cannot be found."""
    # Only the real base branch. `origin/HEAD` is deliberately NOT tried: in a
    # shallow clone it resolves to the fetched tip, so merge-base returns HEAD
    # itself, the diff is empty, and the check passes having compared nothing --
    # a skip wearing a green tick, which is the failure mode this file exists to
    # avoid. Better to find no base and say so.
    for ref in ("origin/main", "main"):
        code, out = _git("merge-base", ref, "HEAD")
        if code == 0 and out:
            return out
    return None


def _entries_at(rev: str | None, source: Path) -> dict[str, dict[str, Any]]:
    """Entries keyed by name, read at `rev` (or from the working tree)."""
    rel = source.relative_to(REPO_ROOT)
    if rev is None:
        text = source.read_text(encoding="utf-8")
    else:
        code, text = _git("show", f"{rev}:{rel}")
        if code != 0:
            return {}
    data = yaml.safe_load(text) or {}
    return {e["name"]: e for e in _tracked(data) if isinstance(e.get("name"), str)}


def _python_paths(entry: dict[str, Any]) -> set[str]:
    paths = {entry.get("python_path")}
    for member in (entry.get("methods") or []) + (entry.get("properties") or []):
        if isinstance(member, dict):
            paths.add(member.get("python_path"))
    return {"src/" + p for p in paths if isinstance(p, str)}


class TestAHashChangeIsJustified:
    """Moving a `matlab_last_sync_hash` with no port needs a written reason.

    `matlab_last_sync_hash` means "I examined this version of this file".
    Nothing can verify that anyone did. Three ways it goes wrong -- missing,
    stale, and *written without reading the diff* -- and only the third is
    silent AND green. The other two turn a build red, so they get fixed; this
    one asserts "reviewed and current" and nothing contradicts it.

    NDR-python lost a whole MATLAB feature to it (their issue #23): a commit
    backfilled hashes onto 20 bridge files, touching zero Python, recording
    what MATLAB's HEAD looked like rather than the commit whose content had
    been ported. Intan multi-file recording support was in that HEAD, was
    never ported, and CI stayed green for four months.

    So: **if an entry's hash changed and none of that entry's python_path
    files changed with it, its decision_log must also have changed and must
    name the new hash.** Porting the change needs nothing extra -- this bites
    only the "nothing to do here" case, which is a decision and belongs in
    writing.

    WHAT IT CANNOT DO. It cannot verify anybody read anything. It makes the
    claim explicit, specific and attributable -- a sentence in the entry,
    naming a commit, visible in review. That is the honest ceiling, and the
    reason the drift message carries the same warning in prose: a check that
    can be satisfied by writing one sentence is worth having precisely because
    writing that sentence is the moment somebody has to think.

    Compared against the MERGE-BASE and the WORKING TREE, not base..HEAD --
    the two disagree on uncommitted edits, and reading new state from one and
    the file list from the other makes the check inert against exactly the
    edits somebody is about to commit.
    """

    def test_a_moved_hash_without_a_port_is_explained(self):
        import os

        base = _merge_base()
        if base is None:
            if os.environ.get("DID_BRIDGE_CHECK_STRICT"):
                pytest.fail(
                    "no merge-base against origin/main -- cannot tell which "
                    "entries this change touches. A shallow clone causes this; "
                    "CI checks this repo out with fetch-depth: 0. "
                    "(DID_BRIDGE_CHECK_STRICT is set, so the history was "
                    "supposed to be there -- skipping would report a check that "
                    "could not run as one that passed.)"
                )
            pytest.skip("no merge-base (shallow clone); cannot diff this branch")

        code, out = _git("diff", "--name-only", base)
        assert code == 0, "git diff failed"
        changed = set(out.split())

        offenders = []
        for source in BRIDGE_FILES:
            if str(source.relative_to(REPO_ROOT)) not in changed:
                continue
            before = _entries_at(base, source)
            after = _entries_at(None, source)
            for name, entry in after.items():
                new_hash = entry.get("matlab_last_sync_hash")
                old_entry = before.get(name)
                if old_entry is None:  # a brand-new entry is a port, not a bump
                    continue
                if new_hash == old_entry.get("matlab_last_sync_hash"):
                    continue
                if _python_paths(entry) & changed:
                    continue  # the port came with it
                log = entry.get("decision_log") or ""
                if log == (old_entry.get("decision_log") or ""):
                    offenders.append(
                        f"{name} ({source.name}): hash -> {new_hash} with no "
                        "Python change and no decision_log change"
                    )
                elif str(new_hash) not in log:
                    offenders.append(
                        f"{name} ({source.name}): decision_log changed but does "
                        f"not name the new hash {new_hash}"
                    )

        assert not offenders, (
            "these entries moved their sync hash with no port and no written "
            "reason:\n  " + "\n  ".join(offenders) + "\n\n"
            "A hash records an examination. If the MATLAB change is genuinely a "
            "no-op on the Python side, that is a decision -- say so in the "
            "entry's decision_log and name the commit. If it is not a no-op, "
            "port it. Bumping alone converts a red build into a false record."
        )


class TestEveryTrackedEntryStatesItsStatus:
    """`status` is required, so "is this ported?" is answered in each entry's
    own text rather than inferred from an absence.

    Repo-local, so it runs in every matrix job -- an entry added without a
    status is caught by the cheap check as well as the MATLAB-dependent one.
    """

    @pytest.mark.parametrize("source", BRIDGE_FILES, ids=lambda p: p.name)
    def test_no_tracked_entry_omits_status(self, source: Path):
        data = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
        offenders = [
            entry.get("name", "<unnamed>")
            for entry in _tracked(data)
            if not entry.get("status")
        ]
        assert not offenders, (
            f"{source.name}: entries with no status:\n  "
            + "\n  ".join(offenders)
            + f"\n\nEvery tracked entry carries one of {list(checker.STATUSES)}. "
            "An absent status is an error, not a plain port."
        )


class TestEveryTrackedEntryCanShowDrift:
    """An entry with a `matlab_path` and no `matlab_last_sync_hash` is exempt
    from the drift gate by omission.

    It asserts its port is current forever and nothing can contradict it --
    the same false assurance as a stale hash, but silent instead of red.
    Settled as decision 2 of NDI-python #211.

    WHY THIS LIVES HERE AND NOT ONLY IN check_bridge_coverage.py. That script's
    `hash` check enforces two rules at once: that the field is PRESENT, and
    that its value resolves to a real commit. Only the second needs a
    DID-matlab checkout. The first reads nothing but this repo, so keeping it
    exclusively in the MATLAB-dependent bridge job would leave the cheaper,
    more basic rule enforced in exactly one place -- and a hashless entry is
    the kind of omission that arrives with a hurried port, which is when a job
    is most likely to be the one that got skipped. Checked in both places on
    purpose: here in every matrix job, there against DID-matlab.
    """

    @pytest.mark.parametrize("source", BRIDGE_FILES, ids=lambda p: p.name)
    def test_a_tracked_matlab_path_carries_a_sync_hash(self, source: Path):
        data = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
        offenders = [
            entry.get("name", "<unnamed>")
            for entry in _tracked(data)
            if entry.get("matlab_path") and not entry.get("matlab_last_sync_hash")
        ]
        assert not offenders, (
            f"{source.name}: entries naming a matlab_path with no "
            f"matlab_last_sync_hash:\n  " + "\n  ".join(offenders) + "\n\n"
            "Without one the entry can never drift, so it claims to be current "
            "forever. Record the MATLAB commit you examined: "
            "git -C <DID-matlab> log -1 --format=%h -- src/did/<matlab_path>"
        )


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

    def test_ported_is_a_written_value_not_an_absence(self):
        """`ported` is written out like any other status.

        It was implicit once, and the cost was legibility: 60 of 62 entries
        said nothing, so the commonest state was the only unlabeled one and
        telling a plain port from ported_differently meant noticing a gap.
        If someone makes it implicit again, this fails rather than letting the
        spec and the checker drift apart quietly.
        """
        assert "ported" in checker.STATUSES
        assert "ported" not in checker.REPLACED_STATUSES
        section = INSTRUCTIONS.read_text(encoding="utf-8")
        section = section[section.index("## Status vocabulary") :]
        section = section[: section.index("\n## ", 1)]
        assert "*(absent)*" not in section, (
            "the vocabulary table still describes an absent status as meaning "
            "ported; `status` is required on every entry."
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
