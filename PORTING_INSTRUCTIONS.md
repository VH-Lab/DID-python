# DID MATLAB-to-Python Porting Instructions

This document describes how to keep DID-python synchronized with DID-matlab
using the YAML bridge files.

## Bridge Files

The bridge files live in `src/did/` and define the contract between MATLAB and
Python implementations:

| Bridge file | Scope |
|---|---|
| `did_matlab_python_bridge.yaml` | Core classes: database, document, query, ido, documentservice, binarydoc |
| `did_matlab_python_bridge_implementations.yaml` | Implementation classes: sqlitedb, doc2sql, binarydoc_matfid |
| `did_matlab_python_bridge_file.yaml` | File I/O: fileobj, readonly_fileobj, binaryTable, utilities |
| `did_matlab_python_bridge_util.yaml` | Utilities: databaseSummary, compareDatabaseSummary, fun, datastructures, db, common |

## Checking for Drift

To check whether a MATLAB file has changed since the last Python sync, use the
`matlab_last_sync_hash` field from the bridge YAML:

```bash
# For a single file:
git -C /path/to/DID-matlab log <matlab_last_sync_hash>..HEAD -- <matlab_path>

# Example:
git -C /path/to/DID-matlab log 205d34b..HEAD -- src/did/+did/+file/fileobj.m
```

If the command produces output, the MATLAB file has changed since the last port.

### Bulk drift check

Run this to check all bridge files at once:

```bash
cd /path/to/DID-matlab
for yaml in /path/to/DID-python/src/did/did_matlab_python_bridge*.yaml; do
    echo "=== $(basename $yaml) ==="
    # Extract matlab_path and matlab_last_sync_hash pairs
    python3 -c "
import yaml, sys
with open('$yaml') as f:
    data = yaml.safe_load(f)
for section in ['classes', 'functions']:
    for item in data.get(section, []):
        path = item.get('matlab_path', '')
        sync_hash = item.get('matlab_last_sync_hash', '')
        name = item.get('name', '')
        if path and sync_hash:
            print(f'{name}|src/did/{path}|{sync_hash}')
" | while IFS='|' read name path hash; do
        changes=$(git log --oneline "$hash"..HEAD -- "$path" 2>/dev/null)
        if [ -n "$changes" ]; then
            echo "  DRIFT: $name ($path)"
            echo "$changes" | sed 's/^/    /'
        fi
    done
done
```

## Porting a MATLAB Change to Python

### Step 1: Identify the change

```bash
git -C /path/to/DID-matlab log <sync_hash>..HEAD -- src/did/<matlab_path>
git -C /path/to/DID-matlab diff <sync_hash>..HEAD -- src/did/<matlab_path>
```

### Step 2: Locate the Python counterpart

Use the bridge YAML to find `python_path` and `python_class` / `python_name`.

### Step 3: Apply the change

Follow these conventions when porting:

| MATLAB | Python |
|---|---|
| `camelCase` method names | `snake_case` method names |
| `struct` | `dict` |
| `cell array` | `list` |
| `char` / `string` | `str` |
| `logical` | `bool` |
| `[]` (empty) | `None` or `[]` depending on context |
| `nargin`, `varargin` | `*args`, `**kwargs` |
| `arguments` block | Type hints + validation |
| Name-value pairs | `**kwargs` |
| 1-based indexing | 0-based indexing |

### Step 4: Update the bridge YAML

After porting, update the entry in the bridge YAML:

1. Set `matlab_last_sync_hash` to the current MATLAB commit hash for that file:
   ```bash
   git -C /path/to/DID-matlab log -1 --format="%h" -- src/did/<matlab_path>
   ```
2. Remove `matlab_current_hash` and `out_of_sync` / `out_of_sync_reason` if present.
3. Update the `decision_log` with the sync date.

### Step 5: Run symmetry tests

```bash
# Python tests
pytest -m make_artifacts -v
pytest -m read_artifacts -v
```

If MATLAB is available, run the full 3-step symmetry cycle:
1. MATLAB `makeArtifacts` tests
2. Python `makeArtifacts` + `readArtifacts` tests
3. MATLAB `readArtifacts` tests

## Bridge YAML Field Reference

| Field | Required | Description |
|---|---|---|
| `name` | Yes | MATLAB function/class name |
| `type` | Yes | `class` or `function` |
| `matlab_path` | Yes | Path relative to `src/did/` in DID-matlab |
| `matlab_last_sync_hash` | Yes | Short SHA of the MATLAB commit last ported to Python |
| `matlab_current_hash` | No | Current MATLAB hash when out of sync (for tracking) |
| `python_path` | Yes | Path relative to `src/did/` in DID-python |
| `python_class` | If class | Python class name |
| `python_name` | If function | Python function name |
| `inherits_matlab` | No | MATLAB parent class(es) |
| `inherits_python` | No | Python parent class(es) |
| `out_of_sync` | No | `true` if MATLAB has diverged |
| `out_of_sync_reason` | No | Human-readable explanation of the divergence |
| `decision_log` | Yes | Sync status, dates, deviation rationale |
| `properties` | No | List of property mappings |
| `methods` | No | List of method mappings |

## Adding a New MATLAB File

When a new file is added to DID-matlab that needs a Python counterpart:

1. Create the Python implementation following the conventions above.
2. Add an entry to the appropriate bridge YAML file.
3. Set `matlab_last_sync_hash` to the MATLAB commit that introduced the file.
4. Run symmetry tests to verify cross-language compatibility.

## Current Sync Status

Last drift check: **2026-08-28**, against DID-matlab `83646a7` (2026-07-25).

Three bridge entries showed drift; each was investigated and resolved as
described below. All other entries are clean.

### `emptystruct` — no Python change needed (bridge hash bumped to `13463d1`)

MATLAB `13463d1` ("Refactor varargin to arguments blocks") added an
`arguments (Repeating)` block and changed `cell2struct(..., fields')` to
`cell2struct(..., fields(:))`. Both are MATLAB-language details — argument
validation syntax and the row/column orientation of a cellstr. Python's
`empty_struct(*field_names)` returns an empty `dict` and has neither concept.
**Behaviorally in sync.**

### `sqlitedb` — already in sync (bridge hash bumped to `14439fb`, `out_of_sync` cleared)

MATLAB `14439fb` (audit 6.1-3) added a private `escapeSqlLiteral()` helper and
applied it at the 14 sites where `sqlitedb.m` interpolates `branch_id` /
`doc_id` / `document_id` / `filename` into double-quoted SQL literals. Its
commit message states it was authored in lockstep with the DID-python half of
the same audit item, which is already in this repo:

- `SQLiteDB` binds every branch/doc/file identifier as a `?` parameter, so
  `sqlite3` does the escaping.
- The one place that must build SQL text — the search-condition builder, where
  the operator and field name vary — routes its values through `_sql_escape()`
  and validates field names.
- Re-verified 2026-08-28: no unparameterized identifier interpolation remains.
  The only f-string SQL in `sqlitedb.py` builds a list of `?` placeholders.

**No port needed.** The older `websave` → `ndi.cloud.api.files.getFile` change
(`926c430`) also needs no port; see *Not Yet Ported* below.

### `database` — cannot be ported yet (`out_of_sync: true`)

MATLAB PR #153 (`c561c13`..`0142532`, 2026-07-25) changed
`validate_doc_vs_schema()`:

- A schema-declared `depends_on` entry is required to be **present** only when
  the schema marks it `mustbenotempty`. Optional dependencies may now be
  omitted from a document without raising
  `DID:Database:ValidationDependsOn`.
- A missing optional dependency is reported through a new
  `DID:Database:MissingOptionalDependency` warning, which is **opt-in**: it is
  emitted only when the `DID_FORCE_VALIDATION_WARNINGS` environment variable is
  set to a non-zero value, and when enabled it is forced through a caller's
  global `warning('off')`.
- The MATLAB test suite was reclassified accordingly: the `item1`/`item2`/
  `item3` removers and the `invalid name` dependency modifier moved from
  `TestInvalidModification` to `TestValidModification`, and a new
  `TestOptionalDependencyWarning` covers both sides of the env-var gate.

**Python has no counterpart to change.** There is no document-vs-schema
validation anywhere in DID-python: `Database.add_docs()` adds documents without
validating them, `did.database` has no `validate_docs` /
`validate_doc_vs_schema`, and `Document` has no `validate` (see the comment at
the end of `document.py`). Python's `tests/test_valid_modification.py` and
`tests/test_invalid_modification.py` are not the schema-modification suites
their MATLAB namesakes are — they exercise branch/document add-remove behavior —
so the MATLAB test reclassification has no Python analogue either.

Because there is no required-dependency check to relax, there is no code path
in which PR #153 is observable in Python. This is a pre-existing capability gap
that PR #153 widens, not a regression it introduced. The `database` bridge entry
now records `matlab_current_hash: 0142532` with `out_of_sync: true`, and
`validate_docs` / `validate_doc_vs_schema` are listed there as not implemented.

**To close this gap**, port the validation subsystem as one unit — it needs
schema loading, recursive superclass validation, and per-field type checks, all
of which `Document.validate` also requires. When it is ported, the
optional-dependency semantics above must be implemented at the same time so the
two languages agree on which documents are valid.

### Bridge coverage gaps found and fixed

A drift check only covers files that have a bridge entry with a sync hash, so
coverage was audited in both directions on 2026-08-28.

**Python side: complete.** Every module under `src/did/` is referenced by some
bridge entry's `python_path`, except the three `__init__.py` package markers
(two empty, one re-exporting `did.util`).

**MATLAB side: two files were invisible to the drift check.** `dumbjsondb.m`
and `fileCache.m` sat in `not_applicable` in `bridge_file.yaml`, which means no
`matlab_last_sync_hash` and therefore no drift detection — while their own
rationales asserted that Python equivalents exist. Both rationales were wrong,
in opposite directions:

- **`dumbjsondb`** — stays `not_applicable`, rationale corrected. DID-python's
  backend is SQLiteDB and there is no plan to port the JSON-file document
  store. The `DumbJsonDB` class in `file.py` arrived with the initial bulk port
  commit (`97ba45c`) and is dead code: nothing in `src/` or `tests/` imports or
  exercises it, and it has no binary-file, search, remove, clear, `alldocids`,
  or metadata support. It is a vestigial stub, not a port, and the old
  rationale ("has a Python equivalent") overstated it. Either delete the class
  or, if a JSON backend is ever wanted, promote it to a tracked entry and port
  it properly.
- **`fileCache`** — promoted to a tracked class entry, `out_of_sync: true`.
  Not a MATLAB divergence (`fileCache.m` is unchanged since `3aa892d`); the
  Python side is a stub, and it is duplicated. There are two unrelated classes
  named `FileCache`: `file.py:FileCache` implements construction plus
  `.fileCacheInfo` read/write and nothing else — no `addFile`, `removeFile`,
  `isFile`, `fileList`, `resizeAndAdd`, `touch`, or `clear`, so nothing can be
  cached and `maxSize`/`reduceSize` are stored but never enforced — while
  `common.py:FileCache` is a three-line placeholder holding only
  `(path, size)`. **`did.common.get_cache()` returns the `common.py`
  placeholder**, so the object DID-python hands callers is the emptier of the
  two, where MATLAB's `getCache()` returns a working `did.file.fileCache`.
  Fixing this means collapsing the two classes and implementing the cache
  operations.

Two further entries were making claims that did not hold and were corrected:

- `getCache` in `bridge_util.yaml` read "Exact match"; the function shape
  matches but the returned object does not (see above).
- The `did.datastructures.table_cross_join (duplicate)` entry in
  `bridge_util.yaml` claimed MATLAB has `tableCrossJoin` in both
  `+did/+datastructures/` and `+did/+db/`. It exists only in `+did/+db/`;
  there is no duplicate.

The remaining unbridged MATLAB files are correctly out of scope: `filesep.m`
and `toolboxdir.m` (`not_applicable` entries, MATLAB-only utilities) and
`Contents.m` (a MATLAB toolbox version listing, not code).

### Previously resolved (no Python changes needed)

The following MATLAB changes (March 29-31, 2026) were verified to already be
handled correctly by Python:

- **fileobj / readonly_fileobj / binaryTable / binarydoc_matfid**: MATLAB
  changed default permission `'r'` -> `'rb'` for Linux binary-mode
  compatibility. Python's `Fileobj.fopen()` already appends `'b'` to the mode
  string if not present (line 88-89 of `file.py`), so all files are opened in
  binary mode regardless. **Behaviorally in sync.**
- **fileobj fread**: MATLAB changed default precision from `'char'` to
  `'uint8'`. Python's `fread()` returns raw `bytes`, which is equivalent to
  `uint8`. **No change needed.**
- **fileobj fwrite**: MATLAB updated permission check to allow `'r+'` mode.
  Python relies on native file objects to reject writes on read-only files.
  **No change needed.**
- **mustBeValidPermission**: MATLAB added binary-mode variants. Python's
  `must_be_valid_permission()` already accepts `rb`, `wb`, `ab`, etc.
  **Already in sync.**

## Not Yet Ported from MATLAB

These MATLAB features do not yet have Python counterparts:

| MATLAB feature | Bridge file | Priority |
|---|---|---|
| `database.validate_docs` | bridge.yaml | High |
| `database.validate_doc_vs_schema` (incl. optional `depends_on` semantics and the `DID_FORCE_VALIDATION_WARNINGS`-gated warning, MATLAB PR #153) | bridge.yaml | High |
| `document.validate` | bridge.yaml | Medium |
| `database.exist_doc` | bridge.yaml | Medium |
| `binaryTable` write methods | bridge_file.yaml | Medium |
| `fileCache` cache operations (`addFile`, `removeFile`, `isFile`, `fileList`, `resizeAndAdd`, `touch`, `clear`) — and collapsing the duplicate `FileCache` in `common.py` and `file.py` so `get_cache()` returns a working cache | bridge_file.yaml | Medium |
| Remote (URL) file locations in `open_doc` — MATLAB uses `ndi.cloud.api.files.getFile`; Python would need `requests`/`urllib` | bridge_implementations.yaml | Low |
| `database.freeze_branch` | bridge.yaml | Low |
| `database.is_branch_editable` | bridge.yaml | Low |
| `database.display_branches` | bridge.yaml | Low |
| `database.close_doc` | bridge.yaml | Low |
| `document.dependency_value_n` | bridge.yaml | Low |
| `document.add_dependency_value_n` | bridge.yaml | Low |
| `document.remove_dependency_value_n` | bridge.yaml | Low |
