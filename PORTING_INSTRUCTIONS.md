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

`bin/check_bridge_coverage.py` runs the drift check over every bridge entry,
along with the coverage checks below:

```bash
python bin/check_bridge_coverage.py --matlab-repo /path/to/DID-matlab
```

It defaults to `../DID-matlab`, and also reads `DID_MATLAB_REPO`. Use
`--check` to run one check at a time (`file`, `hash`, `drift`, `member`,
`missing`); it exits non-zero if anything is reported.

### What the coverage checks are for

A drift check only sees a MATLAB file that some bridge entry names *and* gives
a `matlab_last_sync_hash`. Anything else is invisible: a MATLAB change to it
will never show up. The script checks that the bridge has no such blind spots.

| Check | What it enforces |
|---|---|
| `file` | Every `.m` file under `DID-matlab/src/did` is either tracked by an entry or listed under `not_applicable`; every `.py` module under `src/did` is some entry's `python_path`. Both `matlab_path` and `python_path` point at files that exist. |
| `hash` | Every entry with a `matlab_path` has a `matlab_last_sync_hash`, and that hash is a real commit. An entry without one can never show drift. |
| `drift` | No MATLAB commits touch a tracked file after its sync hash. |
| `member` | Every method/property entry names a symbol that exists in the MATLAB class (or one it inherits from), and its `python_name` exists in the Python class. |
| `missing` | Every public MATLAB method and property has a bridge entry. Protected, private and `delete` members are skipped as implementation detail. |

CI runs `file`, `hash`, `member` and `missing` as gating checks, and `drift`
non-gating: drift turns red when DID-matlab moves, which no commit here causes
and none can fix until someone does the port.

### Member entry conventions

The `member` check relies on two fields, both required for it to mean anything:

- **`python_name`** — the Python identifier, or `~` when the member is not
  ported. Without it the mapping is prose only and nothing can verify it.
- **`python_path`** — only when the counterpart lives outside the entry's own
  `python_path`. `database.validate_doc_vs_schema` maps to `did/validate.py`,
  for example.
- **`matlab_name: ~`** — for a Python-only member. Every member's `name` is the
  MATLAB name, so a Python-only addition needs the marker or it reads as an
  entry pointing at a MATLAB method that does not exist.

One entry must describe one member. A combined `name: "a / b / c"` hides
whether `b` and `c` are bridged at all.

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

Each entry in `methods` / `properties` takes:

| Field | Required | Description |
|---|---|---|
| `name` | Yes | MATLAB method/property name. One entry per member — never `"a / b"` |
| `python_name` | Yes | Python identifier, or `~` when not ported |
| `matlab_name` | No | `~` marks a Python-only member with no MATLAB counterpart |
| `python_path` | No | Only when the counterpart lives outside the entry's `python_path` |
| `kind` | No | `constructor`, `static` or `hidden` |
| `decision_log` | Yes | Deviations, sync date, rationale |
| `input_arguments` / `output_arguments` | No | Argument type mappings |

## Adding a New MATLAB File

When a new file is added to DID-matlab that needs a Python counterpart:

1. Create the Python implementation following the conventions above.
2. Add an entry to the appropriate bridge YAML file.
3. Set `matlab_last_sync_hash` to the MATLAB commit that introduced the file.
4. Run symmetry tests to verify cross-language compatibility.

## Current Sync Status

Last drift check: **2026-08-28**, against DID-matlab `83646a7` (2026-07-25).
Run `python bin/check_bridge_coverage.py` to re-check; it currently reports
zero problems across all five checks.

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

### `database` — ported: Python now has schema validation

MATLAB PR #153 (`c561c13`..`0142532`, 2026-07-25) changed
`validate_doc_vs_schema()`:

- A schema-declared `depends_on` entry is required to be **present** only when
  the schema marks it `mustbenotempty`. Optional dependencies may be omitted
  without raising `DID:Database:ValidationDependsOn`.
- A missing optional dependency is reported through a new
  `DID:Database:MissingOptionalDependency` warning, **opt-in**: emitted only
  when `DID_FORCE_VALIDATION_WARNINGS` is set to a non-zero value, and then
  forced through a caller's global `warning('off')`.

Python had **no document-vs-schema validation at all**, so there was nothing to
port the change into. Rather than record a permanent gap, the validation
subsystem was ported (2026-08-28). See *Validation* below.

### Bridge coverage audit, 2026-08-28

Coverage was audited in both directions and is now enforced by
`bin/check_bridge_coverage.py` (above), which reports zero problems against
DID-matlab `83646a7`. The audit found the bridge described the port accurately
in prose but was not *checkable*, and had several blind spots.

**No drift.** Every tracked entry is current against MATLAB `83646a7`.

**Blind spots closed:**

- **`validate.py` was not referenced by any entry.** The largest recent port —
  the whole validation subsystem — had no `python_path` pointing at it. It is
  now reached through `python_path` on the `database` method entries that map
  into it (`get_document_schema`, `validate_doc_vs_schema`,
  `validate_field_type_and_value`, `checkfiles`, `isfilenamematch`,
  `canfindonefile`). The claim in the previous audit that every Python module
  was referenced stopped being true when `validate.py` was added.
- **`matlabdumbjsondb` had no `matlab_last_sync_hash`** — the same blind spot
  found in `dumbjsondb` and `fileCache` last time, missed because that audit
  checked the `not_applicable` lists but not tracked entries with a missing
  hash. It is tracked now; unported does not have to mean unwatched.
- **`Contents.m`** was already excused but the coverage check did not recognize
  the entry: `not_applicable` names appear bare (`Contents.m`), by stem, and
  dotted (`did.file.dumbjsondb`), and the matcher handled only the last two.

**Ported but untracked.** Real ported code no bridge entry mentioned, so a
MATLAB change to any of it would have gone unnoticed:

| Member | Python counterpart |
|---|---|
| `binaryTable.getLock` / `releaseLock` / `lockFileName` / `tempFileName` / `rowSize` | `get_lock` / `release_lock` / `lock_file_name` / `temp_file_name` / `row_size` |
| `query.searchcellarray2searchstructure` | `search_cell_array_to_search_structure` |
| `query.searchstruct` | `Query._create_search_structure` |
| `sqldb.alldocids` | `Database.all_doc_ids` |
| all six `PathConstants` properties | `PATH`, `DEFPATH`, `DEFINITIONS`, `temppath`, `filecachepath`, `preferences` |

Writing these up surfaced two deviations that had never been recorded.
`search_cell_array_to_search_structure` is a simplification: MATLAB dispatches
on the value's MATLAB class, while Python picks `exact_number` for an int or
float and `regexp` for everything else, so a cell/list, struct/dict or logical
that MATLAB handles falls through to `regexp`. And `PathConstants` enforces
writability with a `mustBeWritable` property validator in MATLAB, checked once
at class load, versus a `@property` calling `must_be_writable()` on every read
in Python.

**Unported and untracked.** Public MATLAB API with no Python counterpart *and*
no bridge entry, so the gap was recorded nowhere:

- `database.get_preference_names` / `get_preference` / `set_preference`. Python
  has the `preferences` dict but no accessors, so preferences can only be
  reached by touching the attribute directly. Added to *Not Yet Ported* below.
- `database.findfilematch`. No port needed — `check_files` does the same
  matching inline through `is_filename_match` — but that reasoning was written
  down nowhere.
- `binaryTable.compare`, and MATLAB's Hidden singular-form aliases
  (`add_doc`, `get_doc`, `remove_doc`, `display_branch`, `get_parent_branch`).

**Entries corrected:**

- `binaryTable.insertRow` claimed "Python: (partial implementation)". There is
  no `insert_row` in `BinaryTable` at all.
- Three entries packed several members into one `name` field
  (`checkfiles / isfilenamematch / canfindonefile`, `open_db / close_db`,
  `doc2sql / doc_to_sql`) — the last of which was not even two MATLAB methods
  but a MATLAB name and a Python name in one string. All are now one entry per
  member.
- `sqlitedb.get_docs_by_branch` is Python-only and is marked `matlab_name: ~`.

**Checked and correct:** all 52 tracked entries are current; the symmetry test
suites correspond one-to-one (`tests/symmetry/{make,read}_artifacts/database/`
against `tests_symmetry/+did/+symmetry/+{make,read}Artifacts/+database/`); and
`filesep.m`, `toolboxdir.m` and `dumbjsondb.m` remain correctly
`not_applicable`.

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

## Validation

`src/did/validate.py` is the Python counterpart of the validation MATLAB
performs in `did.database`. The live entry point is the same in both languages:
`database.add_docs`, which validates by default (`validate=True` in Python,
`'Validate', true` in MATLAB) and can be turned off per call.

| MATLAB | Python |
|---|---|
| `database.validate_docs` | `Database.validate_docs` → `validate.validate_docs` |
| `database.get_document_schema` | `validate.get_document_schema` (+ `resolve_definition_path`) |
| `database.validate_doc_vs_schema` | `validate.validate_doc_vs_schema` |
| `database.validate_field_type_and_value` | `validate.validate_field_type_and_value` |
| `database.checkfiles` | `validate.check_files` |
| `database.isfilenamematch` | `validate.is_filename_match` |
| `database.canfindonefile` | `validate.can_find_one_file` |

What it checks, per document: `document_class` exists and carries a non-empty
`class_name`, `property_list_name` and `class_version`; a document whose
`document_class` has no `validation` field is skipped (that is how a class opts
out); otherwise the schema is loaded and the document is checked for classname,
superclasses (recursing into each superclass schema), `depends_on`, `file`, and
each class-specific property list, with every field checked against its declared
type — `integer`, `double`, `matrix`, `timestamp`, `char`/`string`, `did_uid`,
`structure`, `cell`.

Failures raise `ValidationError`, which carries MATLAB's error identifier as
`.identifier` (e.g. `'DID:Database:ValidationDependsOn'`), so both languages can
be branched on with the same strings. PR #153's semantics are included:
optional dependencies may be absent, and
`MissingOptionalDependencyWarning` is raised only under
`DID_FORCE_VALIDATION_WARNINGS`. MATLAB forces that warning past a global
`warning('off')` by switching the identifier on for the duration; Python's
equivalent is a `catch_warnings` block with an `always` filter.

Deviations, all recorded in the bridge YAML:

- **Timestamps** — MATLAB parses via `java.time.LocalDateTime`, Python via
  `datetime.fromisoformat` with `strptime` fallbacks.
- **`can_find_one_file`** — MATLAB pre-checks an `http` location with a HEAD
  request. Python has no URL download path, so a URL counts as findable and is
  resolved when actually read (which is MATLAB's own behavior for any other
  non-file location).
- **Journalling** — MATLAB's `add_docs` disables SQLite journalling when
  validation is off. Python does not; its `sqlite3` connection is managed
  differently.

### Two bugs validation exposed immediately

Turning validation on surfaced two divergences that had been invisible:

**1. Document IDs were the wrong format.** `base.schema.json` types `base.id` as
`did_uid`, which requires MATLAB's `did.ido` format: 16 hex digits, an
underscore, 16 more. Python's `IDO.unique_id()` returned a **UUID4**, so *every*
Python-generated document was schema-invalid and would have been rejected by
MATLAB's `add_docs`. UUID4 is also not sortable by creation time, which
`did.ido` guarantees. `ido.py` now generates
`num2hex(serial_date_number) + '_' + num2hex(rand)` like MATLAB, and `is_valid`
enforces MATLAB's rule. (The `ido` bridge entry had recorded this divergence as
acceptable because "both guarantee uniqueness" — it was not.) Deviation: MATLAB
takes the serial date number from `clock`, which is local time; Python uses UTC,
as `did.ido`'s own documentation specifies. IDs are only ever compared for
equality across languages, never parsed back into a time.

**2. Documents were built from the wrong file.**
`Document.read_blank_definition` read `database_schema/<class>.schema.json` —
the *validation schema* — as though it were the class definition. A new document
therefore carried the schema's field-descriptor list as its property list,
superclasses as bare strings, and no `document_class.validation` at all, so
there was no pointer to validate against. It now reads
`database_documents/<class>.json` and merges superclass definitions recursively
like MATLAB's `readblankdefinition`, so a `demoB` document carries the `base`,
`demoA` and `demoB` property lists. Fixing that in turn exposed
`_reset_file_info`, which only cleared `files.file_info` when it was *absent*
and so let a definition's template file entries (`demoFile.json` ships two) leak
into every new document; it now clears unconditionally, as MATLAB does.

Coverage is in `tests/test_validation.py` (34 tests), mirroring MATLAB's
`TestOptionalDependencyWarning` and the reclassified `depends_on` rows of
`TestValidModification` / `TestInvalidModification`.

## Not Yet Ported from MATLAB

These MATLAB features do not yet have Python counterparts. (Schema
validation was ported 2026-08-28 and is no longer listed; MATLAB's
`document.validate` is dead code and is deliberately not ported — see the
document entry in `bridge.yaml`.)

| MATLAB feature | Bridge file | Priority |
|---|---|---|
| `database.exist_doc` | bridge.yaml | Medium |
| `database.get_preference` / `set_preference` / `get_preference_names` — Python has the `preferences` dict but no accessors for it | bridge.yaml | Medium |
| `binaryTable` write methods | bridge_file.yaml | Medium |
| `fileCache` — cache operations, the duplicate `FileCache` in `common.py` vs `file.py`, **and an incompatible on-disk format**; see "The file cache" below | bridge_file.yaml | **Medium-High** |
| Remote (URL) file locations in `open_doc` — MATLAB uses `ndi.cloud.api.files.getFile`; Python would need `requests`/`urllib` | bridge_implementations.yaml | Low |
| `database.freeze_branch` | bridge.yaml | Low |
| `database.is_branch_editable` | bridge.yaml | Low |
| `database.display_branches` | bridge.yaml | Low |
| `database.close_doc` | bridge.yaml | Low |
| `binaryTable.compare` (only needed by `findRow`, also unported) | bridge_file.yaml | Low |
| `query.searchcellarray2searchstructure` handles only numbers and strings; MATLAB also handles cells, structs and logicals | bridge.yaml | Low |

### Enumerated dependency lists (`name_1`, `name_2`, ...) — ported 2026-08-28

`dependency_value_n`, `add_dependency_value_n` and `remove_dependency_value_n`
were listed as Low priority. That was wrong. They were ported on 2026-08-28
after testing showed the missing half was the *write* side, and that it failed
silently:

1. **Reading**: `dependency_value('item')` raised on a document holding
   `item_1`, `item_2`, `item_3` — a caller had to guess suffixes and probe.
2. **Writing**: the only way to append was
   `set_dependency_value(name, value, error_if_not_found=False)`, which wrote
   an entry named literally `item`, not `item_4`.
3. **Neither validator caught it.** The schema declares the un-enumerated stem
   and `_strip_enumeration_suffix('item')` returns `'item'`, so the malformed
   entry matched the stem. Building both documents and running `validate_docs`
   confirmed: the well-formed list and the corrupted one *both* pass.
4. **MATLAB then could not see it.** `dependency_value_n` stops at the first
   gap, so the entry Python wrote was invisible — silent cross-language data
   loss on an ordinary call, with no error on either side.

Two things changed alongside the port. `set_dependency_value` now refuses to
append a stem-named entry when an enumerated list exists, pointing the caller
at `add_dependency_value_n` — a deliberate divergence, since MATLAB appends
without complaint and that is how the corruption arose. And matching in
`dependency_value` / `set_dependency_value` is now case-insensitive, mirroring
MATLAB's `strcmpi`; it had been case-sensitive, so a MATLAB-written `Item_1`
was unreachable from Python by `item_1`. Coverage is in
`tests/test_dependency_lists.py` (23 tests); MATLAB has none for these methods.

## Re-audit of the remaining Low-priority items, 2026-08-28

Every remaining item was retested rather than taken on trust, since the
enumerated-lists entry had been mis-ranked. The rankings below now rest on
evidence.

### Confirmed Low, with the evidence that was missing

- **`freeze_branch` / `is_branch_editable`** — MATLAB's `frozen_branch_ids` is
  an in-memory property never written to the database, enforced in exactly one
  place (`delete_branch`). A freeze does not survive a session and cannot cross
  languages, so this is an absent feature, not a divergence. The sub-branch
  half of `is_branch_editable` is upheld in Python by a SQLite FOREIGN KEY
  anyway; what is missing is the ability to *ask* before trying.
- **`close_doc`** — a thin delegate to `do_close_doc`. Python's file objects
  close on garbage collection.
- **`display_branches` / `display_branch`** — display helpers, no data effect.
- **`fileobj.fscanf` / `fprintf`** — Python uses native file I/O instead.

### Re-ranked: `document.eq` is not "not yet ported", it is **do not port**

MATLAB's `eq` compares `document_properties.did_document.id`. No such field
exists anywhere in either repo — documents carry `base.id` — so calling it on a
real document raises "Reference to non-existent field". It is dead code, like
`document.validate`, and listing it as "not yet ported" invited someone to port
a bug. Noted for Python callers: with no `__eq__`, `doc_a == doc_b` falls back
to identity, so two `Document` objects holding the same `base.id` compare
unequal. If an id comparison is ever wanted, write it against `base.id`.

### New: methods marked "Exact match" whose Python side is a stub

These are **not** on any gap list, because the methods exist in both languages.
The `member` coverage check cannot catch them either — it verifies that a
counterpart exists, not that it does the same thing. Four `Database` methods
carry a `# Validation logic would go here` placeholder where MATLAB validates:

| Method | What actually happens |
|---|---|
| `delete_branch` | **Deleting a non-existent branch is a silent no-op.** No schema backstop, because there is no row to constrain. The sub-branch guard exists only as a FOREIGN KEY, raising `sqlite3.IntegrityError` where MATLAB raises `DID:Database:ParentBranch`. |
| `set_branch` | Accepts a branch that does not exist; the error surfaces later at `add_docs`. MATLAB fails fast. Fail-deferred, not fail-silent. |
| `add_branch` | Duplicate id and missing parent are caught by UNIQUE / FOREIGN KEY constraints, so data stays correct but the error type differs. |
| `get_doc_ids` | Same placeholder; MATLAB validates the branch id first. |

Also `BinaryTable.read_row`, marked "Synchronized": MATLAB decodes the row
using `recordType` and `elementsPerColumn`, while Python returns the raw bytes
of one column and does neither. And `Document.set_properties` describes itself
as "a simplified way to set properties" under a parity claim; treat it as
unverified.

Nothing in **DID-python's** `src/` or `tests/` constructs a `BinaryTable`, a
`FileCache` or a `DumbJsonDB`, which is why a stubbed `read_row` could sit
under a "Synchronized" note unnoticed. That is a fact about the Python side
only — in DID-matlab two of the three are live. See below.

### Cross-cutting: the error identifier asymmetry

Porting validation deliberately gave `ValidationError` an `.identifier`
carrying MATLAB's error id, so both languages branch on the same strings. That
pattern only half-exists: the branch and database operations above raise bare
`ValueError` or `sqlite3.IntegrityError` with no identifier, so a caller cannot
branch on `DID:Database:ParentBranch` the way it can on
`DID:Database:ValidationDependsOn`. Worth closing as one piece of work rather
than method by method.

### Suggested next step

Port `validate_branch_id` / `validate_doc_id` and call them from the four
stubbed methods, raising an identifier-carrying error as the validation port
does. That closes the one genuinely silent failure (`delete_branch`), converts
three constraint errors into MATLAB-matching identifiers, and makes
`set_branch` fail fast — in one change with one set of tests.

## The file cache — corrected 2026-08-28

An earlier note in this document said `BinaryTable`, `FileCache` and
`DumbJsonDB` were "dead code". That is true of **DID-python only**. Checking
DID-matlab shows two of the three are live there, which changes the
assessment:

- **`binaryTable` is the storage engine behind `fileCache`.** `fileCache.m`
  constructs one over the cache index and calls `readHeader`, `writeHeader`,
  `getLock`, `findRow` and `releaseLock` — several of which were among the
  untracked methods this audit added.
- **`fileCache` is on the document-open path.** `sqlitedb.do_open_doc`
  consults `filecachepath` when opening a document's binary file and calls
  `didCache.touch()` on a hit. It is not a side feature.
- **`dumbjsondb`** backs `matlabdumbjsondb`, an alternative database backend
  that DID-python deliberately does not implement. That one really is out of
  scope on both sides.

### The part that is a trap

Both languages call the cache index `.fileCacheInfo`. They write different
things into it:

| | MATLAB | Python |
|---|---|---|
| Format | binary, via `binaryTable` | JSON |
| Header | 26 bytes: `fileNameCharacters` (uint16), then `maxSize`, `reduceSize`, `currentSize` (uint64) | JSON keys |
| Rows | fixed-width `{char[n], double, uint64}` — filename, last-accessed, size | a `files` object |

Neither can read the other's file. They do not collide today only because
`PathConstants.filecachepath` resolves differently: MATLAB uses
`fullfile(userpath, ...)`, and `userpath` is not the home directory (typically
`<home>/Documents/MATLAB`), while Python uses `Path.home()`. That divergence
was itself mis-recorded in the bridge as a mere validation-style difference
until 2026-08-28. (The `userpath` default is quoted from its documentation;
this audit could not run MATLAB to confirm it.)

**So the order of work matters.** Aligning the paths looks like a one-line fix
and is the obvious first move — and it is the wrong one. It would point both
languages at a single directory in which each corrupts the other's index.
Align the format first, then the path.

The Python-side cache is dead code today, so nothing is broken right now. The
risk is entirely in what a reasonable next change would do.

## What actually happens across languages on the same dataset

Traced 2026-08-28, prompted by the question "if MATLAB caches files for a
dataset and Python then opens it, do the caches conflict?". Short answer: no,
not today — but for a worse reason than compatibility.

**Python has no cache on the read path at all.** `SQLiteDB.open_doc` resolves
the location directly (absolute, or relative to the database directory) and
returns a `ReadOnlyFileobj`. It never calls `get_cache()` and never looks at
`filecachepath`. MATLAB's `do_open_doc` consults the cache and calls `touch()`
on a hit. So the two never meet: MATLAB maintains a cache Python does not read
or write, and Python re-resolves every file.

That means the practical consequence is lost caching, not corruption. The
corruption scenario needs the two to share a directory, which today they do
not — and which is exactly why the paths must not be "fixed" first. See "The
file cache" above.

One caveat: MATLAB's `filecachepath` is `fullfile(userpath, ...)`, and
`userpath` is user-configurable. If it were ever set to the home directory —
or left empty, which makes MATLAB's path *relative* and resolved against the
current working directory — the two could land in the same place without
anyone intending it.

### Remote (URL) file locations: both languages reject them

Worth stating plainly, since it is the case that matters most for a
cloud-hosted dataset. A document whose only location for a file is an
`http(s)` URL **fails validation in both languages**:

- Python's `can_find_one_file` returns False for a URL by design — there is no
  URL download path, so it could not resolve one later either.
- MATLAB *intends* to HEAD-check the URL and accept a reachable one, but
  `canfindonefile` calls `req.send(url)` where `url` is never assigned
  anywhere in `database.m`. The error is swallowed by a bare `catch`, so the
  branch can never report found.

They agree by accident, not by design. **This is a live bug in DID-matlab**
(`src/did/+did/database.m`, in `canfindonefile`): the argument should be
`fileLocation`. It has presumably masked itself, because the symptom is a
remote file being reported "missing" rather than an error.

Fixing it would immediately split the two languages: MATLAB would start
accepting reachable URLs that Python still rejects. So the Python side needs a
real reachability check, and a URL download path behind it, at the same time.
Both halves are in "Not Yet Ported"; the MATLAB fix should not land alone.

Note also that the earlier bridge entry and the `can_find_one_file` docstring
both described the opposite behavior — that Python treats a URL as findable —
until this was actually run. Corrected 2026-08-28.