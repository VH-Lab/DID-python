# Instructions for AI Agents

## Overview

DID-python is a Python port of [DID-matlab](https://github.com/VH-Lab/DID-matlab)
(Data Interface Database). MATLAB is the source of truth; Python mirrors it.

## Where the rules live

**[`PORTING_INSTRUCTIONS.md`](PORTING_INSTRUCTIONS.md) is normative.** This file
is a map to it, not a summary of it — a rule written in two places is a rule
that will end up disagreeing with itself, and the bridge spec in the sibling
repository NDI-python did exactly that. When something here and something there
appear to conflict, `PORTING_INSTRUCTIONS.md` is right and this file is stale.

| You need to know | Read |
|---|---|
| What the bridge YAML files are and what each field means | § Bridge YAML Field Reference |
| Whether something is ported, and how to say it isn't | § Status vocabulary |
| Which MATLAB files are deliberately untracked | § `not_tracked` |
| What `matlab_last_sync_hash` holds, and what a squash merge does to it | § `matlab_last_sync_hash` is a commit |
| How to port a MATLAB change | § Porting a MATLAB Change to Python |
| What CI checks, and why one check does not gate | § What the coverage checks are for |

## Architecture

- **Lead–follow.** DID-matlab leads. A Python behaviour that differs from
  MATLAB's is a deviation, and a deviation belongs in the entry's
  `decision_log` — not only in a commit message, which nobody reading the
  bridge will find.
- **Bridge contract.** `src/did/did_matlab_python_bridge*.yaml` records, per
  MATLAB class/function, where its Python counterpart is or why there isn't
  one. It is the answer to "is this ported?".
- **Naming.** MATLAB `camelCase` maps to Python `snake_case`
  (`allDocIds` → `all_doc_ids`). This differs from the sibling repo NDR-python,
  which preserves MATLAB names verbatim; do not carry that convention across.

## Workflow

1. Check the bridge YAML entry for what you are touching.
2. Port the change.
3. Update the entry — `matlab_last_sync_hash`, `status`, `decision_log` —
   following § Step 4. Never bump a hash whose diff you did not read: the
   field's only claim is that somebody looked at it.
4. If a MATLAB file will *not* be ported, it still needs recording, with a
   `status` and a reason. The `file` check fails on an unrecorded `.m` file.
5. Run everything CI runs, before pushing (§ Step 5):

   ```bash
   black src/ tests/
   ruff check src/ tests/
   pytest
   python bin/check_bridge_coverage.py --matlab-repo ../DID-matlab
   ```

   `bin/check_bridge_coverage.py` needs a DID-matlab checkout that is **not
   shallow** — it reads history backwards from each sync hash. Set
   `DID_MATLAB_REPO` instead of passing `--matlab-repo` if you prefer.

## Two failure modes worth naming

- **A check that does not run passes.** The bridge CI job names each check on
  the command line, so adding one to `CHECKS` in the script is half the job.
  Rules that need no MATLAB checkout belong in
  `tests/test_bridge_contract.py`, which runs in every test-matrix job.
- **A test that skips is green.** A bridge or symmetry test that skips when its
  inputs are missing reports success for a comparison it never made. Where the
  inputs are guaranteed, fail instead — that is what
  `DID_SYMMETRY_REQUIRE_ARTIFACTS=1` does for the read-artifact tests, and what
  the shallow-clone guard does for the hash and drift checks.

## Environment

- Python 3.10+ (CI tests 3.10, 3.11 and 3.12)
- `black==26.5.1` and `ruff==0.16.5`, pinned to match CI
