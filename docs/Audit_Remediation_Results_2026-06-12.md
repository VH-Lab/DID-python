# DID-python Audit Remediation — Results (2026-06-12)

> **Context for a reviewer / next agent.** One of **9 coordinated PRs** in the 2026-06 NDI
> ecosystem audit; **none are merged.** This repo's PR: **VH-Lab/DID-python#23** — **merge
> together with the DID-matlab PR VH-Lab/DID-matlab#146** (same audit item §6.1; isa +
> SQL hardening land in lockstep). The one deferral is the **`timestamp` format DECISION**
> (see "DECISION required" below).

Branch `audit/did-python-2026-06`, off `origin/main` (`1b1491f`). This is the
Python half of the **DID lockstep** (DID-python + DID-matlab change together so a
cross-language symmetry run stays consistent); the MATLAB half is
`audit/did-matlab-2026-06`.

## Findings addressed (audit §6.1)

| # | Severity | Commit | Summary |
|---|----------|--------|---------|
| 6.1-1 | Critical | `76ae1bb` | **isa operator parity.** The brute-force `field_search` isa used a `param1 in a` heuristic plus an exact `class_name` check, so it (a) missed superclass membership — a probe document did not match `isa('element')` — and (b) spuriously matched a class name that was merely an incidental top-level field of an unrelated document. Same query, different result set per language. The brute-force isa now derives the class and superclass names from the **same** `did.implementations.doc2sql` helpers the SQL path uses, so both paths agree and both follow MATLAB's `isa(X)` = (X is the class) OR (X is a superclass) semantics. |
| 6.1-3 | Medium | `76ae1bb` | **SQL field-name hardening.** The query field name was interpolated into the SQL text (`fields.field_name = '<field>'`, and into `LIKE` patterns, so it cannot be a bound parameter). It is now restricted to `^[A-Za-z0-9_.]+$`; an out-of-charset field name returns `None` and the caller falls back to the injection-free brute-force search. |
| 6.1-4 | Low | `4ae15de` | **Missing sqlite indexes.** `doc_data` (one row per field per document, the largest table) had no index, so every search did a full-scan nested-loop join. Adds `doc_data(value)` (the index the DID-MATLAB reference already has), plus the more targeted `doc_data(field_idx, value)` and `doc_data(doc_idx)`. Created `IF NOT EXISTS` on every open, so databases downloaded before this fix benefit on next use. |
| 6.1-8 | Medium | (this commit) | **No LICENSE.** Added `LICENSE` (CC BY-NC-SA 4.0) matching the DID-matlab counterpart. |

### Correction to the audit's 6.1-1 wording

The audit said the SQL isa path matched `meta.class`/`meta.superclass` — "fields that
never exist in stored docs." In fact `doc2sql.py` **does** produce `meta.class` and
`meta.superclass` (mirroring MATLAB `doc2sql.m`), so the SQL isa path was already
correct; the real divergence was the brute-force path, fixed above. The "never exist"
observation applies only to a **MATLAB-written `ndi.db`** where the `doc_data` search
cache is left unpopulated (0 rows) — a separate issue (the search cache is a derived
index; authoritative content is `docs.json_code`), not an isa field-name bug.

## DECISION required — do not merge a serialization change without sign-off

**§6.1-2 / §7.3-13: the `timestamp` column meaning diverges across languages and is
NOT changed here.** DID-matlab writes MATLAB `now` (datenum — days since year 0) into
the `docs`/`branches`/`branch_docs` `timestamp REAL` column; DID-python writes
`time.time()` (Unix epoch seconds) into the same column. A document written by one
client and compared with `lessthan`/`greaterthan` on `timestamp` by the other silently
gives wrong results, and the cloud backend stores the value verbatim (it has no opinion).

This is a **cross-client format decision**, not a bug to patch on one side — changing
the serialization on either side alone would break the other and any already-stored
data. It needs an explicit decision (proposed: **ISO-8601 TEXT**, or Unix epoch seconds,
with a documented one-time conversion/migration for existing rows) made jointly for
DID-python, DID-matlab, and the cloud backend. Left untouched pending that decision.

## Validation

`PYTHONPATH=src python -m pytest tests/ --ignore=tests/symmetry` = **62 passed** (was
55; +7 from the new `tests/test_isa_parity.py`), black + ruff clean. The new tests cover
isa across own-class / superclass-descendant / root-superclass / unrelated, an
incidental-field trap, SQL-vs-brute-force agreement, and a field-name injection attempt
that does not inject.

## Lockstep / merge

Merge with `audit/did-matlab-2026-06` (the MATLAB SQL-injection escaping is the same
audit item 6.1-3). The isa change brings Python to parity with MATLAB (MATLAB's isa was
already the correct reference; no MATLAB isa change was needed).
