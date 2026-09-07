#!/usr/bin/env python3
"""Audit the DID MATLAB<->Python bridge YAML files for coverage and drift.

The bridge files in ``src/did/did_matlab_python_bridge*.yaml`` are the contract
between DID-matlab and DID-python. They are only useful if they are *complete*:
a MATLAB file, method or property that no bridge entry mentions is invisible to
the drift check, so a MATLAB change to it will never be noticed on the Python
side.

This script checks that completeness in both directions:

  file      every .m file under DID-matlab/src/did is either tracked by an
            entry or explicitly listed under ``not_tracked``; every .py
            module under DID-python/src/did is some entry's ``python_path``.
  hash      every tracked entry carries a ``matlab_last_sync_hash`` (without
            one it can never show drift) and that hash is a real commit.
  status    every ``status`` is one of the documented values, says why in a
            ``decision_log``, and agrees with the rest of its entry; every
            ``not_tracked`` entry actually excuses a MATLAB file, or declares
            that it does not.
  drift     no MATLAB commits touch a tracked file after its sync hash.
  member    every method/property entry names a symbol that really exists in
            the MATLAB class, and its ``python_name`` really exists in the
            Python class -- or is explicitly null, meaning "not ported".
  missing   every public MATLAB method/property has a bridge entry.

Usage:
    python bin/check_bridge_coverage.py [--matlab-repo PATH] [--check CHECK]...

Exits non-zero if any check reports a problem, so it can gate CI.
"""

from __future__ import annotations

import argparse
import ast
import glob
import os
import re
import subprocess
import sys

import yaml

CHECKS = ("file", "hash", "status", "drift", "member", "missing")

# The bridge's status vocabulary. This tuple is the mechanism; the normative
# definition of each value lives in PORTING_INSTRUCTIONS.md ("Status
# vocabulary"), and tests/test_bridge_contract.py asserts the two agree, so a
# value cannot be added here without being documented (or vice versa).
#
# "ported" is deliberately absent: it is the DEFAULT, spelled as no `status`
# plus a `python_path`. Allowing it to also be written out would make a
# missing status ambiguous between "ported" and "nobody filled this in",
# which is the ambiguity the field exists to remove.
STATUSES = ("ported_differently", "porting_deferred", "matlab_only", "retired")

# Statuses an entry in a `not_tracked` list may carry. `ported_differently`
# is excluded: if Python has the capability, the entry belongs in `classes`
# or `functions` where the drift check can see it.
NOT_TRACKED_STATUSES = ("porting_deferred", "matlab_only", "retired")

# Status names that were used and then replaced, mapped to what to write
# instead. A retired name gets a targeted error saying what it became, rather
# than a bare "not in the vocabulary" that leaves the reader to guess.
#
# This matters more than it looks: the vocabulary is shared by DID-python,
# NDI-python and NDR-python, and the three drifted apart once already. Someone
# arriving from a repo (or a doc) that still carries an old name should be told
# the new one, not just told no. `not_applicable` is here because it was this
# repo's own key name until 2026-09-07, and it conflated three claims -- so
# there is no single replacement to name, and the message says so.
REPLACED_STATUSES = {
    "ported_elsewhere": "ported_differently (NDR-python issue #21: python_path "
    "already says WHERE; the manner is the part a reader cannot recover)",
    "not_yet_ported": "porting_deferred",
    "not_applicable": "matlab_only, porting_deferred or retired -- pick the one "
    "you mean; not_applicable conflated all three",
    "implemented": "no status at all (ported is the default: no status plus a "
    "python_path)",
    "does_not_exist": "retired",
    # "ported" has no replacement NAME because the default has no name. Writing
    # it out is the specific mistake this vocabulary is built to prevent: if the
    # value were sometimes explicit, an absent status would stop meaning
    # "ported" and start meaning "ported, or nobody filled this in".
    "ported": "no status at all (a plain port is the default: no status plus a "
    "python_path)",
}

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Entries permitted to be drifted right now, by `name`. The drift check gates
# (NDI-python issue #211, one policy for all three repos), and a repo switching
# it on with a backlog would go red on work nobody has done yet -- so the
# rollout is a RATCHET: list what is already drifted, gate everything else, and
# shrink the list as entries get reviewed.
#
# The ratchet shape is decision 3 of #211 and is NOT yet settled (the
# alternative is clean the backlog first, then switch on). It is moot here --
# this repo had no backlog to do either to -- so this list is the anticipated
# shape, not a decided one, and is the piece to revisit if #211 lands
# differently.
#
# DID-python's list is empty and is expected to stay that way: at the time the
# gate went on, zero entries were drifted, so there was no backlog to ratchet
# down. It exists so the escape hatch is present when a batch of MATLAB work
# lands faster than it can be reviewed -- add the name, port or review it, take
# the name out again. It is not a place to park an entry indefinitely; an
# allowlisted entry that has STOPPED drifting is reported, so the list cannot
# quietly outlive its reason.
DRIFT_ALLOWLIST: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# MATLAB source parsing
# ---------------------------------------------------------------------------

# Keywords that open a block terminated by a matching `end`. `methods` and
# `properties` are handled separately because we also need their attributes.
_OPENERS = (
    "if",
    "for",
    "while",
    "switch",
    "try",
    "parfor",
    "function",
    "events",
    "enumeration",
    "arguments",
    "spmd",
    "classdef",
    "methods",
    "properties",
)
_OPEN_TOKEN = re.compile(r"\b(" + "|".join(_OPENERS) + r")\b")
_END_TOKEN = re.compile(r"\bend\b")
_METHODS_OPEN = re.compile(r"^\s*methods\b(.*)$")
_PROPERTIES_OPEN = re.compile(r"^\s*properties\b(.*)$")

# `function [a,b] = name(args)` / `function a = name(args)` / `function name(args)`
_FUNCTION = re.compile(
    r"^\s*function\s+(?:\[[^\]]*\]\s*=\s*|[\w~]+\s*=\s*)?([A-Za-z_]\w*)\s*[\(\s]?"
)
# An abstract method is a bare signature with no `function` keyword:
#   `obj = fopen(obj)` / `fseek(obj, location, reference)`
_ABSTRACT_SIG = re.compile(
    r"^\s*(?:\[[^\]]*\]\s*=\s*|[\w~]+\s*=\s*)?([A-Za-z_]\w*)\s*\("
)
# A property declaration: a name optionally followed by size/type/validators
# and a default, e.g. `recordSize uint16 {mustBeVector} = zeros(1,0)`.
_PROPERTY = re.compile(r"^\s*([A-Za-z_]\w*)\s*(?:\(|\{|=|[A-Za-z_]|;|$)")


def _strip_comment(line: str) -> str:
    """Drop a trailing MATLAB comment, respecting single-quoted strings."""
    out = []
    in_str = False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "'":
            # '' inside a string is an escaped quote
            if in_str and i + 1 < len(line) and line[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_str = not in_str
        elif ch == "%" and not in_str:
            break
        out.append(ch)
        i += 1
    return "".join(out).rstrip()


def _outside_brackets(line: str) -> str:
    """Blank out anything inside (), [] or {}.

    Two constructs would otherwise corrupt the block-depth count: `end` used as
    an index (`names(end+1)`) is not a block terminator, and a keyword appearing
    inside a string or argument list is not a block opener.
    """
    out = []
    depth = 0
    in_str = False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "'" and not in_str and (not out or out[-1] in " ,(=[{"):
            in_str = True
        elif ch == "'" and in_str:
            if i + 1 < len(line) and line[i + 1] == "'":
                i += 2
                continue
            in_str = False
            out.append(" ")
            i += 1
            continue
        if in_str:
            out.append(" ")
            i += 1
            continue
        if ch in "([{":
            depth += 1
            out.append(" ")
        elif ch in ")]}":
            depth = max(0, depth - 1)
            out.append(" ")
        else:
            out.append(" " if depth else ch)
        i += 1
    return "".join(out)


def _depth_delta(line: str) -> int:
    """Net change in block nesting contributed by one logical line.

    Counting tokens rather than only line-leading keywords is what makes a
    one-line `if tf, x = 1; end` net out to zero instead of leaking a level.
    """
    bare = _outside_brackets(line)
    return len(_OPEN_TOKEN.findall(bare)) - len(_END_TOKEN.findall(bare))


def _logical_lines(path: str):
    """Yield MATLAB source lines with block comments dropped and `...` joined."""
    pending = ""
    in_block_comment = False
    with open(path, errors="replace") as handle:
        for raw in handle:
            # MATLAB block comments (`%{` ... `%}` alone on their lines) often
            # wrap commented-out code. Skipping them keeps dead code from being
            # reported as live API.
            stripped = raw.strip()
            if in_block_comment:
                if stripped == "%}":
                    in_block_comment = False
                continue
            if stripped == "%{":
                in_block_comment = True
                continue

            line = _strip_comment(raw)
            if not line.strip():
                if pending:
                    yield pending
                    pending = ""
                continue
            if line.rstrip().endswith("..."):
                pending += line.rstrip()[:-3] + " "
                continue
            yield pending + line
            pending = ""
    if pending:
        yield pending


def _attrs(text: str) -> str:
    """Summarize a methods/properties block's attributes as a short label."""
    access = re.search(r"(?:Set|Get)?Access\s*=\s*([A-Za-z]+)", text)
    parts = [access.group(1).lower() if access else "public"]
    if re.search(r"\bAbstract\b", text, re.IGNORECASE):
        parts.append("abstract")
    if re.search(r"\bHidden\b", text, re.IGNORECASE):
        parts.append("hidden")
    if re.search(r"\bStatic\b", text, re.IGNORECASE):
        parts.append("static")
    return ",".join(parts)


def parse_matlab_class(path: str) -> tuple[dict[str, str], dict[str, str]]:
    """Return ({method: attrs}, {property: attrs}) for a MATLAB classdef file.

    Blocks are tracked by nesting depth so that a nested helper function or a
    `methods (Access=protected)` block is attributed correctly -- getting this
    wrong is what makes a naive grep report protected internals as public API.
    """
    methods: dict[str, str] = {}
    properties: dict[str, str] = {}
    depth = 0
    block: str | None = None  # attrs of the open methods/properties block
    kind: str | None = None  # "methods" or "properties"
    block_depth: int | None = None

    for line in _logical_lines(path):
        if block is None:
            match = _METHODS_OPEN.match(line)
            if match:
                block, kind, block_depth = _attrs(match.group(1)), "methods", depth
                depth += _depth_delta(line)
                continue
            match = _PROPERTIES_OPEN.match(line)
            if match:
                block, kind, block_depth = _attrs(match.group(1)), "properties", depth
                depth += _depth_delta(line)
                continue
        elif kind == "methods":
            # Only count definitions at the block's own level; anything deeper
            # is a nested helper function, not a method.
            match = _FUNCTION.match(line)
            if match:
                if depth == block_depth + 1:
                    methods.setdefault(match.group(1), block)
            elif "abstract" in block and depth == block_depth + 1:
                match = _ABSTRACT_SIG.match(line)
                if match:
                    methods.setdefault(match.group(1), block)
        elif kind == "properties" and depth == block_depth + 1:
            match = _PROPERTY.match(line)
            if match and match.group(1) != "end":
                properties.setdefault(match.group(1), block)

        depth += _depth_delta(line)
        if block is not None and depth <= block_depth:
            block = kind = block_depth = None

    return methods, properties


def parse_matlab_function(path: str) -> str:
    """Return the name of the top-level function defined in a MATLAB file."""
    with open(path, errors="replace") as handle:
        for raw in handle:
            match = _FUNCTION.match(_strip_comment(raw))
            if match:
                return match.group(1)
    return ""


# ---------------------------------------------------------------------------
# Python source parsing
# ---------------------------------------------------------------------------


def parse_python(path: str) -> tuple[dict[str, set[str]], set[str]]:
    """Return ({class: {methods}}, {module-level functions}) for a .py file."""
    with open(path) as handle:
        tree = ast.parse(handle.read())
    classes: dict[str, set[str]] = {}
    module: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            classes[node.name] = {
                child.name
                for child in node.body
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            module.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    module.add(target.id)
    return classes, module


def python_attributes(path: str, class_name: str) -> set[str]:
    """Return instance attributes assigned as `self.x = ...` in a class."""
    with open(path) as handle:
        tree = ast.parse(handle.read())
    found: set[str] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name == class_name):
            continue
        # Class-level constants (`CACHE_INFO_FILE_NAME = ...`) and annotated
        # class attributes are part of the class surface, not just `self.x`.
        for stmt in node.body:
            if isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        found.add(target.id)
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                found.add(stmt.target.id)
        for sub in ast.walk(node):
            if isinstance(sub, ast.Assign):
                for target in sub.targets:
                    if (
                        isinstance(target, ast.Attribute)
                        and isinstance(target.value, ast.Name)
                        and target.value.id == "self"
                    ):
                        found.add(target.attr)
            elif isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                found.add(sub.name)
    return found


# ---------------------------------------------------------------------------
# Bridge loading
# ---------------------------------------------------------------------------


def load_bridge(repo: str):
    """Load every bridge YAML; return (tracked entries, not_tracked entries)."""
    tracked, not_tracked = [], []
    for path in sorted(glob.glob(os.path.join(repo, "src/did/*.yaml"))):
        with open(path) as handle:
            data = yaml.safe_load(handle)
        name = os.path.basename(path)
        for section in ("classes", "functions"):
            for item in data.get(section) or []:
                item["_bridge"] = name
                item["_section"] = section
                tracked.append(item)
        for item in data.get("not_tracked") or []:
            item["_bridge"] = name
            not_tracked.append(item)
    return tracked, not_tracked


def excuses(entry_name: str, matlab_path: str) -> bool:
    """Does a `not_tracked` entry name this MATLAB file?

    `not_tracked` names appear bare (`Contents.m`), by stem (`filesep`), or
    dotted (`did.file.dumbjsondb`), so all three spellings resolve here rather
    than at each call site.
    """
    base = os.path.basename(matlab_path)
    stem = base[:-2] if base.endswith(".m") else base
    return entry_name in (base, stem) or entry_name.endswith("." + stem)


class Report:
    def __init__(self) -> None:
        self.problems: list[tuple[str, str]] = []

    def add(self, check: str, message: str) -> None:
        self.problems.append((check, message))

    def section(self, check: str, title: str) -> None:
        rows = [m for c, m in self.problems if c == check]
        print(f"\n=== {title} ===")
        if not rows:
            print("  OK")
        for row in rows:
            print(f"  {row}")


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def git(matlab_repo: str, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", matlab_repo, *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def check_file_coverage(report, repo, matlab_repo, tracked, not_tracked):
    matlab_files = [
        p for p in git(matlab_repo, "ls-files", "src/did").split() if p.endswith(".m")
    ]
    covered = {"src/did/" + e["matlab_path"] for e in tracked if e.get("matlab_path")}
    excused = {e["name"] for e in not_tracked}

    for path in sorted(matlab_files):
        if path in covered:
            continue
        if any(excuses(name, path) for name in excused):
            continue
        report.add("file", f"MATLAB file has no bridge entry: {path}")

    for entry in tracked:
        rel = entry.get("matlab_path")
        if rel and not os.path.exists(os.path.join(matlab_repo, "src/did", rel)):
            report.add(
                "file",
                f"{entry['name']}: matlab_path does not exist: src/did/{rel}",
            )

    python_files = [
        p for p in git(repo, "ls-files", "src/did").split() if p.endswith(".py")
    ]
    referenced = set()
    for entry in tracked:
        for candidate in [entry.get("python_path")] + [
            m.get("python_path")
            for m in (entry.get("methods") or [])
            if isinstance(m, dict)
        ]:
            if candidate:
                referenced.add("src/" + candidate)

    for path in sorted(python_files):
        if path in referenced or os.path.basename(path) == "__init__.py":
            continue
        report.add("file", f"Python module has no bridge entry: {path}")

    for entry in tracked:
        rel = entry.get("python_path")
        if rel and not os.path.exists(os.path.join(repo, "src", rel)):
            report.add(
                "file",
                f"{entry['name']}: python_path does not exist: src/{rel}",
            )


def check_hashes(report, matlab_repo, tracked):
    for entry in tracked:
        sync = entry.get("matlab_last_sync_hash")
        if not entry.get("matlab_path"):
            continue
        if not sync:
            report.add(
                "hash",
                f"{entry['name']} ({entry['_bridge']}): no matlab_last_sync_hash, "
                "so drift can never be detected",
            )
            continue
        probe = subprocess.run(
            ["git", "-C", matlab_repo, "cat-file", "-e", f"{sync}^{{commit}}"],
            capture_output=True,
            check=False,
        )
        if probe.returncode != 0:
            report.add(
                "hash",
                f"{entry['name']}: matlab_last_sync_hash {sync} is not a commit "
                "in DID-matlab",
            )


def _decision_log_words(entry) -> int:
    return len((entry.get("decision_log") or "").split())


def check_status(report, matlab_repo, tracked, not_tracked):
    """Validate the status vocabulary and the claims each value makes.

    Before this check the bridge had no `status` field at all. What stood in
    for it was prose in fields the script parses as paths -- `python_path:
    "(not separately implemented)"` on sqldb, `"(not applicable)"` on
    matlabdumbjsondb -- which the loader skipped on the leading "(", so the
    two entries read identically to every check while meaning opposite things
    (sqldb's capability exists in Python, merged into Database; the other's
    does not exist at all). A status a checker cannot read is a comment.
    """
    for entry in tracked:
        where = f"{entry['name']} ({entry['_bridge']})"
        status = entry.get("status")
        if status is None:
            # The default, "ported": no status plus a real python_path. An
            # entry with neither says nothing at all about the Python side.
            if not entry.get("python_path"):
                report.add(
                    "status",
                    f"{where}: no status and no python_path, so the entry makes "
                    f"no claim. Add a python_path, or a status from {list(STATUSES)}",
                )
            continue

        if status in REPLACED_STATUSES:
            report.add(
                "status",
                f"{where}: {status!r} is a retired status name; "
                f"use {REPLACED_STATUSES[status]}. "
                "See PORTING_INSTRUCTIONS.md, 'Retired status names'.",
            )
            continue
        if status not in STATUSES:
            report.add(
                "status",
                f"{where}: unknown status {status!r}; "
                f"expected one of {list(STATUSES)} (or none, meaning ported). "
                "See PORTING_INSTRUCTIONS.md, 'Status vocabulary'.",
            )
            continue

        if _decision_log_words(entry) < 5:
            report.add(
                "status",
                f"{where}: status {status} with no decision_log explaining it. "
                "A recorded gap with no reason gets re-investigated, which is "
                "what recording it was supposed to prevent.",
            )

        if status == "ported_differently" and not entry.get("python_path"):
            report.add(
                "status",
                f"{where}: ported_differently must give the python_path where "
                "the capability actually lives, or it is indistinguishable "
                "from porting_deferred.",
            )
        if status in ("porting_deferred", "matlab_only") and entry.get("python_path"):
            report.add(
                "status",
                f"{where}: status {status} says there is no Python counterpart, "
                f"but python_path names one ({entry['python_path']}). If the "
                "capability exists in Python, the status is ported_differently.",
            )

    matlab_files = [
        p for p in git(matlab_repo, "ls-files", "src/did").split() if p.endswith(".m")
    ]
    covered = {"src/did/" + e["matlab_path"] for e in tracked if e.get("matlab_path")}
    uncovered = [p for p in matlab_files if p not in covered]

    for entry in not_tracked:
        where = f"{entry.get('name', '<unnamed>')} ({entry['_bridge']})"
        status = entry.get("status")
        if status in REPLACED_STATUSES:
            report.add(
                "status",
                f"{where}: {status!r} is a retired status name; "
                f"use {REPLACED_STATUSES[status]}. "
                "See PORTING_INSTRUCTIONS.md, 'Retired status names'.",
            )
            continue
        if status not in NOT_TRACKED_STATUSES:
            report.add(
                "status",
                f"{where}: not_tracked entries need a status from "
                f"{list(NOT_TRACKED_STATUSES)}, got {status!r}. "
                "See PORTING_INSTRUCTIONS.md, 'Status vocabulary'.",
            )
            continue
        if _decision_log_words(entry) < 5:
            report.add(
                "status",
                f"{where}: status {status} with no decision_log explaining it.",
            )

        # An entry that excuses no MATLAB file is inert: it reads as a coverage
        # exemption and covers nothing. Two of the six here were exactly that,
        # and nothing said so. `retired` and `external: true` are the two ways
        # to declare it deliberately.
        hits = [path for path in uncovered if excuses(entry["name"], path)]
        declared_fileless = status == "retired" or bool(entry.get("external"))
        if not hits and not declared_fileless:
            report.add(
                "status",
                f"{where}: excuses no MATLAB file under src/did, so it exempts "
                "nothing while reading as an exemption. Fix the name, or mark "
                "it `status: retired` (the file is gone / never existed) or "
                "`external: true` (it names a file outside DID-matlab).",
            )
        if hits and entry.get("external"):
            report.add(
                "status",
                f"{where}: marked external: true but names DID-matlab "
                f"{hits[0]}. Drop the marker.",
            )


def check_drift(report, matlab_repo, tracked):
    """An entry drifts when DID-matlab has commits touching its matlab_path
    after the recorded hash.

    Drift is asked by walking history, not by comparing the recorded hash
    against the file's current last-touching commit. The two differ on a
    legitimate record: an entry synced in a batch carries a repo-wide commit
    that never touched its own file, so an equality test calls it stale when
    nothing about it changed. `git log <hash>..HEAD -- <path>` is empty for
    that entry, and non-empty exactly when the file really moved.
    """
    allowlisted_but_clean = set(DRIFT_ALLOWLIST)
    for entry in tracked:
        sync = entry.get("matlab_last_sync_hash")
        rel = entry.get("matlab_path")
        if not sync or not rel:
            continue
        path = "src/did/" + rel
        if not os.path.exists(os.path.join(matlab_repo, path)):
            continue
        log = git(matlab_repo, "log", "--oneline", f"{sync}..HEAD", "--", path)
        if not log:
            continue
        if entry["name"] in DRIFT_ALLOWLIST:
            allowlisted_but_clean.discard(entry["name"])
            continue
        first = log.splitlines()[0]
        report.add(
            "drift",
            f"{entry['name']} ({path}) changed since {sync}: {first}. "
            "Review the MATLAB diff and either port the behavioral change or "
            "record in the decision_log that there is none, then bump the hash.",
        )

    # A ratchet only ratchets if it tightens. An entry that has stopped drifting
    # -- because someone reviewed it -- must come off the list, or the list
    # keeps exempting an entry nobody is exempting on purpose any more.
    for name in sorted(allowlisted_but_clean):
        report.add(
            "drift",
            f"{name}: in DRIFT_ALLOWLIST but is not drifting. Remove it from "
            "the list in bin/check_bridge_coverage.py.",
        )


def _matlab_surface(entry, matlab_repo, by_name, seen=None):
    """Methods and properties of a MATLAB class, including inherited ones.

    A bridge entry may legitimately list a member the MATLAB class inherits --
    Python's SQLiteDB overrides `search` and `get_docs`, which MATLAB defines
    once on `did.database` -- so the parent classes named by `inherits_matlab`
    are folded in before anything is reported as missing.
    """
    seen = seen if seen is not None else set()
    if entry["name"] in seen:
        return {}, {}
    seen.add(entry["name"])

    rel = entry.get("matlab_path")
    path = os.path.join(matlab_repo, "src/did", rel) if rel else None
    if path and os.path.exists(path):
        methods, properties = parse_matlab_class(path)
    else:
        methods, properties = {}, {}

    for parent in re.split(r"[&,]", entry.get("inherits_matlab") or ""):
        parent = parent.strip().rsplit(".", 1)[-1]
        if not parent or parent not in by_name:
            continue
        up_methods, up_properties = _matlab_surface(
            by_name[parent], matlab_repo, by_name, seen
        )
        for name, attrs in up_methods.items():
            methods.setdefault(name, attrs + ",inherited")
        for name, attrs in up_properties.items():
            properties.setdefault(name, attrs + ",inherited")

    return methods, properties


def _python_index(repo, entry, member=None):
    """Resolve the Python file a bridge entry (or one of its members) targets."""
    rel = (member or {}).get("python_path") or entry.get("python_path")
    if not rel:
        return None, None
    path = os.path.join(repo, "src", rel)
    if not os.path.exists(path):
        return None, None
    return path, rel


def check_members(report, repo, matlab_repo, tracked):
    """Verify each method/property entry against both sources."""
    by_name = {e["name"]: e for e in tracked}
    for entry in tracked:
        rel = entry.get("matlab_path")
        if not rel:
            continue
        matlab_file = os.path.join(matlab_repo, "src/did", rel)
        if not os.path.exists(matlab_file):
            continue

        if entry.get("type") == "class":
            ml_methods, ml_props = _matlab_surface(entry, matlab_repo, by_name)
        else:
            ml_methods, ml_props = {parse_matlab_function(matlab_file): "public"}, {}

        python_class = entry.get("python_class")
        for kind, members, ml_symbols in (
            ("method", entry.get("methods") or [], ml_methods),
            ("property", entry.get("properties") or [], ml_props),
        ):
            for member in members:
                if not isinstance(member, dict):
                    continue
                name = member.get("name", "")
                label = f"{entry['name']}.{name}"

                if "matlab_name" in member and member.get("matlab_name") is None:
                    # Explicitly Python-only: there is no MATLAB side to check.
                    pass
                elif name not in ml_symbols and name != entry["name"]:
                    report.add(
                        "member",
                        f"{label}: bridge lists a {kind} that does not exist in "
                        f"src/did/{rel} (use `matlab_name: ~` if it is "
                        "Python-only)",
                    )

                if "python_name" not in member:
                    report.add(
                        "member",
                        f"{label}: no python_name field, so the mapping cannot "
                        "be verified (use `python_name: ~` when not ported)",
                    )
                    continue

                python_name = member.get("python_name")
                if python_name is None:
                    continue  # explicitly not ported

                path, target = _python_index(repo, entry, member)
                if path is None:
                    continue
                classes, module = parse_python(path)
                pool = set(module)
                if python_class:
                    if python_class in classes:
                        pool |= python_attributes(path, python_class)
                    elif member.get("python_path") is None:
                        report.add(
                            "member",
                            f"{entry['name']}: python_class {python_class} not "
                            f"found in src/{target}",
                        )
                        continue
                for candidate in classes.values():
                    pool |= candidate

                if python_name not in pool:
                    report.add(
                        "member",
                        f"{label}: python_name {python_name!r} not found in "
                        f"src/{target}",
                    )


def check_missing_members(report, repo, matlab_repo, tracked):
    """Report public MATLAB methods/properties that no bridge entry mentions."""
    for entry in tracked:
        rel = entry.get("matlab_path")
        if not rel or entry.get("type") != "class":
            continue
        matlab_file = os.path.join(matlab_repo, "src/did", rel)
        if not os.path.exists(matlab_file):
            continue
        ml_methods, ml_props = parse_matlab_class(matlab_file)

        for kind, members, symbols in (
            ("method", entry.get("methods") or [], ml_methods),
            ("property", entry.get("properties") or [], ml_props),
        ):
            listed = {m.get("name") for m in members if isinstance(m, dict)}
            for name, attrs in sorted(symbols.items()):
                if name in listed or name == entry["name"]:
                    continue
                # Constructors, destructors and non-public members are
                # implementation detail, not part of the ported contract.
                if name == "delete" or "private" in attrs or "protected" in attrs:
                    continue
                report.add(
                    "missing",
                    f"{entry['name']}.{name}: public MATLAB {kind} [{attrs}] "
                    f"has no bridge entry ({entry['_bridge']})",
                )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--matlab-repo",
        default=os.environ.get(
            "DID_MATLAB_REPO", os.path.join(REPO, "..", "DID-matlab")
        ),
        help="path to a DID-matlab checkout (default: ../DID-matlab)",
    )
    parser.add_argument(
        "--check",
        action="append",
        choices=CHECKS,
        help="run only the named check (repeatable); default is all of them",
    )
    args = parser.parse_args()

    matlab_repo = os.path.abspath(args.matlab_repo)
    if not os.path.isdir(os.path.join(matlab_repo, ".git")):
        print(f"error: {matlab_repo} is not a git checkout of DID-matlab")
        print("Pass --matlab-repo PATH or set DID_MATLAB_REPO.")
        return 2

    selected = args.check or list(CHECKS)

    # A shallow checkout has the recent commits and nothing else, so
    # `cat-file -e <older sync hash>` reports "missing" for almost every entry
    # and `log <hash>..HEAD` cannot resolve its left side. The hash and drift
    # checks would then emit one failure per entry -- sixty-odd lines of noise
    # for one setup mistake, which reads as "the bridge is broken" rather than
    # "the clone is shallow". Say it once instead. CI passes fetch-depth: 0;
    # a local `git clone --depth N` needs `git fetch --unshallow`.
    if git(matlab_repo, "rev-parse", "--is-shallow-repository") == "true" and (
        {"hash", "drift"} & set(selected)
    ):
        print(
            f"error: {matlab_repo} is a SHALLOW checkout. The hash and drift "
            "checks walk MATLAB history from each entry's sync hash, and a "
            "shallow clone does not have it -- every entry would be reported "
            "as unresolvable.\n"
            "Run: git -C "
            f"{matlab_repo} fetch --unshallow   (CI uses fetch-depth: 0)"
        )
        return 2

    tracked, not_tracked = load_bridge(REPO)
    report = Report()

    print(f"DID-python : {REPO}")
    print(
        f"DID-matlab : {matlab_repo} @ {git(matlab_repo, 'rev-parse', '--short', 'HEAD')}"
    )
    print(
        f"bridge     : {len(tracked)} tracked entries, "
        f"{len(not_tracked)} not_tracked"
    )

    if "file" in selected:
        check_file_coverage(report, REPO, matlab_repo, tracked, not_tracked)
        report.section("file", "File coverage (both directions)")
    if "hash" in selected:
        check_hashes(report, matlab_repo, tracked)
        report.section("hash", "Sync hashes present and valid")
    if "status" in selected:
        check_status(report, matlab_repo, tracked, not_tracked)
        report.section("status", "Status vocabulary and the claims it makes")
    if "drift" in selected:
        check_drift(report, matlab_repo, tracked)
        report.section("drift", "MATLAB drift since last sync")
    if "member" in selected:
        check_members(report, REPO, matlab_repo, tracked)
        report.section(
            "member", "Method and property entries resolve in both languages"
        )
    if "missing" in selected:
        check_missing_members(report, REPO, matlab_repo, tracked)
        report.section("missing", "Public MATLAB members with no bridge entry")

    print(f"\n{len(report.problems)} problem(s) found.")
    return 1 if report.problems else 0


if __name__ == "__main__":
    sys.exit(main())
