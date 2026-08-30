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
document entry in `bridge.yaml`. `binaryTable`'s write path, `binaryTable.
compare` and `fileCache` were ported 2026-08-29 and are no longer listed — see
"The file cache" below. `database.exist_doc` and `add_docs`'
`customFileHandler` were ported 2026-08-30 and are no longer listed — see
"exist_doc and ingestion at add time" below.)

| MATLAB feature | Bridge file | Priority |
|---|---|---|
| `database.get_preference` / `set_preference` / `get_preference_names` — Python has the `preferences` dict but no accessors for it | bridge.yaml | Medium |
| Remote (URL) file locations in `open_doc` — MATLAB uses `ndi.cloud.api.files.getFile`; Python would need `requests`/`urllib` | bridge_implementations.yaml | Low |
| `database.freeze_branch` | bridge.yaml | Low |
| `database.is_branch_editable` | bridge.yaml | Low |
| `database.display_branches` | bridge.yaml | Low |
| `database.close_doc` | bridge.yaml | Low |
| `query.searchcellarray2searchstructure` handles only numbers and strings; MATLAB also handles cells, structs and logicals | bridge.yaml | Low |
| Deleting ingested copies from the file cache on removal — MATLAB's `do_remove_doc` deletes each `files.cached_location` from disk before dropping the rows (issue #55) | bridge_implementations.yaml | Medium |
| Retiring removed document ids — MATLAB's `deleted_docs` table and the `DID:SQLITEDB:DELETED_DOC` refusal in `do_add_doc` (issue #55) | bridge_implementations.yaml | Medium |

### Issue #55: what landed in MATLAB, and what is left here

MATLAB `edb1a6b` (2026-08-30) closed DID-matlab issue #55, which asked for
three things when a document is removed from its last branch: delete its cached
files, delete its field data, and refuse the id if it is ever added back.

The middle one was already done here and needed no port. `_do_remove_doc` has
deleted the `doc_data`, `files` and `docs` rows since DID-python#39, and MATLAB
adopted the same three deletes in the same order — dependents before the `docs`
row they reference. Python is the language that enforces that order
(`PRAGMA foreign_keys = ON`), which is why the ordering bug surfaced here first
as a document with an attached file refusing to be removed.

The other two are the rows added to the table above. Both are genuinely
missing, not merely unreachable:

- **Cached files.** Local ingestion is still not implemented here, so a
  document added from Python usually has an empty `cached_location` and nothing
  to clean up. But remote ingestion *is* implemented (DID-python#42), it writes
  to `FileDir/<uid>`, and MATLAB `d7ac853` pointed both languages at one shared
  cache directory. A removal from Python therefore orphans a file that a
  removal from MATLAB would have deleted.
- **Retired ids.** MATLAB records the id in a `deleted_docs` table and raises
  `DID:SQLITEDB:DELETED_DOC` on a re-add. Nothing stops the same re-add here.

Neither breaks reading a database across languages. `deleted_docs` is created
on demand, is not one of MATLAB's mandatory tables, and MATLAB does not add it
to a Python-written database merely by opening one.

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

`Document.set_properties` describes itself as "a simplified way to set
properties" under a parity claim; treat it as unverified.

`BinaryTable.read_row` was in this list too — marked "Synchronized" while
returning the raw bytes of one column and decoding nothing. It was ported
properly on 2026-08-29 along with the rest of `BinaryTable` and `FileCache`;
see "The file cache" below. It could sit under a parity claim unnoticed
because nothing in **DID-python's** `src/` or `tests/` constructed a
`BinaryTable`, a `FileCache` or a `DumbJsonDB`. That was a fact about the
Python side only — in DID-matlab two of the three were live — and it is why
the port came with tests that pin the byte layout rather than the round trip.
`DumbJsonDB` remains unexercised by design.

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

## The file cache — ported 2026-08-29

An earlier note in this document called `BinaryTable`, `FileCache` and
`DumbJsonDB` "dead code". That was true of **DID-python only**. In DID-matlab
two of the three are live, which is what made this worth porting rather than
deleting:

- **`binaryTable` is the storage engine behind `fileCache`.** `fileCache.m`
  constructs one over the cache index and calls `readHeader`, `writeHeader`,
  `getLock`, `findRow` and `releaseLock`.
- **`fileCache` is on the document-open path.** `sqlitedb.do_open_doc`
  consults `filecachepath` when opening a document's binary file and calls
  `didCache.touch()` on a hit. It is not a side feature.
- **`dumbjsondb`** backs `matlabdumbjsondb`, an alternative database backend
  that DID-python deliberately does not implement. That one really is out of
  scope on both sides.

### What was wrong

Both languages call the cache index `.fileCacheInfo`, in a directory named by
`PathConstants.filecachepath`. They wrote different things into it:

| | MATLAB | Python (before) |
|---|---|---|
| Format | binary, via `binaryTable` | JSON |
| Header | 26 bytes: `fileNameCharacters` (uint16), then `maxSize`, `reduceSize`, `currentSize` (uint64) | JSON keys |
| Rows | fixed-width `{char[n], double, uint64}` — filename, last-accessed, size | a `files` object that was never written to |

Neither could read the other's file. They did not collide only because
`filecachepath` resolves differently: MATLAB uses `fullfile(userpath, ...)`
and Python uses `Path.home()`. On top of that, Python's `FileCache` was a stub
(construction and info-file read/write, nothing else), there were *two*
unrelated classes named `FileCache` — one in `file.py`, one in `common.py` —
and `get_cache()` returned the emptier of the two.

**The order of work mattered.** Aligning the paths looks like a one-line fix
and was the wrong first move: it would have pointed both languages at one
directory in which each corrupted the other's index. Format first, then path.

### What is there now

- `BinaryTable` decodes and encodes typed values over MATLAB's little-endian
  layout: `read_row`, `insert_row`, `delete_row`, `write_entry`,
  `write_table`, `find_row` (binary search included) and `compare`.
- `FileCache` implements `add_file`, `remove_file`, `is_file`, `file_list`,
  `resize_and_add` (least-recently-used eviction), `touch` and `clear` over a
  `BinaryTable`, writing MATLAB's binary `.fileCacheInfo` rather than JSON.
  Last-access times are MATLAB datenums; `did.file.datenum` computes them.
- The duplicate `common.py:FileCache` is gone. `get_cache()` returns a
  `did.file.FileCache` at `filecachepath`, as MATLAB's `getCache()` returns a
  `did.file.fileCache`.
- `SQLiteDB.open_doc` consults the cache before other locations, touches what
  it finds there, and puts a retrieved file into it — the same shape as
  `do_open_doc`. A cache that cannot be opened degrades to `None` and the open
  proceeds without it: the cache is an optimization, and losing it should cost
  a re-fetch, not the file.
- The lock file is `<file>-lock` in both languages. Python's
  `checkout_lock_file` used to append `.lock` to whatever it was given, so two
  processes guarding one file took two different locks and excluded each other
  not at all — harmless while the caches were separate, not harmless now.

Tests: `tests/test_binary_table.py` and `tests/test_file_cache.py` assert the
byte layout, not only the round trip (a round trip would pass just as happily
on a format MATLAB cannot read); `tests/test_open_doc_locations.py` has a
`TestFileCacheOnOpen` class whose second-open test uses a handler that raises,
so the open can only succeed from the cache.

### The path — settled 2026-08-29

Both languages now name `<home>/Documents/DID/fileCache`. MATLAB's
`did.common.homeDirectory()` reads `USERPROFILE` on Windows and `HOME`
elsewhere, which is what `pathlib.Path.home()` resolves, so the two agree by
construction rather than by two string literals that happen to match.

It was `fullfile(userpath, 'Documents', 'DID', 'fileCache')`, which on a
normal install is `<home>/Documents/MATLAB/Documents/DID/fileCache` — a
doubled `Documents` that reads like the line was written assuming `userpath`
was the home directory. Anything left there is an unused cache and can be
deleted.

The symmetry pair `common.pathAgreement` covers what all the other cache
tests assume. They check the two languages agree on the *contents* of the
shared directory; that one checks they agree on *which* directory. A
divergence there has no symptom — no error, no failing test, just two
half-populated caches and every file fetched twice.

### Known and accepted: the lock is not atomic on the MATLAB side

MATLAB's `checkout_lock_file` tests `isfile(filename)` in its wait loop and
then calls `fopen(filename,'wt')`, which does **not** fail if the file
appeared in between. DID-python uses `open(..., "x")`, which is atomic.

Two MATLAB processes can therefore both write a lock. Since 2026-08-29 they
no longer both *believe* they hold it: `checkout_lock_file` reads the file
back through `did.file.lock_file_key` and stands down unless the surviving
key is its own. The records are a fixed length, so the loser reads a
well-formed file with someone else's key rather than a mangled one.

**The same race between the languages is deliberately left open.** MATLAB's
`'wt'` truncates a lock Python created atomically, and the read-back then
legitimately finds MATLAB's own key, so both proceed. Closing it needs
create-exclusive semantics, which MATLAB's `fopen` does not offer. The
options are a Java dependency in a low-level file utility — at a time when
MathWorks is removing Java from the product — or an on-disk protocol change
(a `mkdir`-based sentinel) that DID-python would have to adopt too.

Neither is a good trade. The window is a few statements wide, contenders are
spread apart by the `pause(1)` in the wait loop, and simultaneous MATLAB
processes have used this scheme for years without trouble. The consequence
is bounded for the file cache — a corrupted index is disposable, clear it
and re-fetch — though not for `dumbjsondb`, which uses the same lock to
guard a document store. Revisit if it ever actually bites.

### Still superseded: the old note on the path

`PathConstants.filecachepath` still differs — MATLAB `fullfile(userpath, ...)`,
Python `Path.home()/Documents/DID/fileCache`. With the formats agreed, making
them match is now safe, but it is a separate change. Note that `userpath` is
user-configurable and can be empty (on a headless runner it is), which makes
MATLAB's path *relative* and resolved against the current working directory —
the CI symmetry run shows exactly that warning. Worth settling deliberately
rather than by defaulting.

## What actually happens across languages on the same dataset

Traced 2026-08-28, prompted by the question "if MATLAB caches files for a
dataset and Python then opens it, do the caches conflict?" — and revisited
2026-08-29 when the cache was ported.

**Before the port, Python had no cache on the read path at all.**
`SQLiteDB.open_doc` resolved the location directly and never called
`get_cache()`. So the two never met: MATLAB maintained a cache Python neither
read nor wrote, and Python re-resolved every file. The practical consequence
was lost caching, not corruption — the corruption scenario needed the two to
share a directory, which is why the paths must not have been "fixed" first.

**Now** both languages read and write the same format, so sharing a directory
is safe and the remaining difference is the path itself (above).

### Remote (URL) file locations — fixed in both repos, 2026-08-28

**Decision: a URL location behaves exactly as a `mustbenotempty == 0` file
does.** Validation does no network I/O; a non-local location is admitted and
its reachability is evaluated when the file is read.

The old behavior, and why it was wrong. `canfindonefile` singled out `http(s)`
and tried to HEAD-check it, so a file the schema marked `mustbenotempty` and
hosted at a URL was rejected as "Missing file" — and `add_docs` then wrote
**nothing**, not a partial document. Two things were wrong with that:

1. **The branch had never worked.** MATLAB's `canfindonefile` called
   `req.send(url)` where `url` is never assigned anywhere in `database.m`. The
   error was swallowed by a bare `catch`, so the URL branch could never report
   found. Python had mirrored the resulting behavior.
2. **It was inconsistent.** `s3://`, `ftp://` and every other non-local scheme
   were accepted without any check. Only `http(s)` was rejected.

The pre-check was therefore **removed rather than repaired**. Repairing it
would have put a network round-trip inside validation and required a URL
download path in DID-python to match. The shipped `demoFile.json` template
carries `location_type: "url"` entries, so remote locations are a first-class
part of the document format; rejecting them was never the intent.

Both repos changed in lockstep:

- `DID-matlab` — `src/did/+did/database.m`, `canfindonefile`: the broken
  `elseif startsWith(fileLocation, 'http')` branch removed, so a non-local
  location falls through to the existing not-pre-checked path.
- `DID-python` — `src/did/validate.py`, `can_find_one_file`: the
  `startsWith('http')` special case removed, same result.

Neither repo had a test covering this. Coverage is now in
`tests/test_validation.py:TestRemoteFileLocations` (7 tests): URL findability,
parity with other non-local schemes, an empty location list, a required
URL-hosted file passing `check_files`, the document being addable, and the URL
surviving the round-trip through SQLite. **MATLAB still has no test for it** —
worth adding on that side.

Behavior now, measured:

| Schema | Location | Result |
|---|---|---|
| `mustbenotempty: 1` or `0` | `https://…`, `s3://…`, other non-local | Valid, not pre-checked |
| any | local path that exists | Valid |
| any | no locations at all | Not findable |

The stored link was never at risk in any of this: a document that gets in
round-trips its locations verbatim through SQLite, URL included. The failure
was only ever about admission.

## Where does the remote-file download live?

Traced 2026-08-28. DID-python has no download path, so a remote file is
unreachable from Python. Looking at how MATLAB does it answers where the
Python equivalent belongs — and the answer is probably *not* DID-python.

`sqlitedb.do_open_doc` dispatches on the location's `type`:

| type | MATLAB does |
|---|---|
| `file` | `copyfile(sourcePath, destPath)` |
| `url` | **`ndi.cloud.api.files.getFile(sourcePath, destPath)`** |
| anything else | calls a caller-supplied `customFileHandler(destPath, sourcePath)`, or errors `DID:SQLITEDB:FileRetrieval:UnsupportedType` |

then adds the result to the file cache and returns a `readonly_fileobj` over
the cached copy.

Two things follow.

**1. DID-matlab depends on NDI.** The `url` branch calls
`ndi.cloud.api.files.getFile` directly — a function that lives in NDI-matlab,
not here. So DID-matlab needs NDI on the path to open a remote file, which is
a layering inversion: the lower-level package calling the higher-level one.
Worth resolving before mirroring it into Python. The cleanest fix on the
MATLAB side is to route `url` through `customFileHandler` too, so NDI supplies
the downloader rather than DID reaching for it.

**2. `customFileHandler` is the extension point, and Python has none.**
`do_open_doc` accepts it as a name-value pair, so a downstream package can
supply file retrieval without DID knowing anything about it. DID-python's
`open_doc(doc_id, filename)` takes no such parameter and no `**kwargs`, so
there is nowhere for a downstream package to hook in.

So the likely shape of the Python work is *not* "add `requests` to
DID-python". It is:

1. Give `open_doc` a `custom_file_handler` parameter mirroring MATLAB's, so a
   downstream package (NDI-python) can supply the downloader. This keeps the
   network dependency out of DID-python entirely.
2. Decide whether DID-python should have a built-in `url` handler at all, or
   whether — as the layering suggests — every remote fetch should go through
   the hook, including in MATLAB.
3. The cache (see above) only becomes worth building once something can
   actually download, since its purpose is to avoid re-fetching.

### How NDI-matlab actually does it (read from VH-Lab/ndi-matlab, 2026-08-28)

The hook is real and already in use. `ndi.database.implementations.database.
didsqlite` calls:

```matlab
db.open_doc(ndi_document_id, filename, 'customFileHandler', @download_file_from_cloud)
```

so NDI supplies retrieval through DID's extension point rather than DID
knowing anything about the cloud. NDI defines three location types
(`ndi.document.add_file`): `file`, `url` for plain `http(s)://`, and
**`ndicloud`** for `ndic://<datasetId>/<fileUid>`. Its handler accepts exactly
`(destPath, sourcePath)`, and for an `ndic://` path it:

1. splits out `datasetId` and `fileUid`;
2. calls `ndi.cloud.api.files.getFileDetails(datasetId, fileUid)` to mint a
   **fresh pre-signed `downloadUrl`**;
3. calls `ndi.cloud.api.files.getFile(fileUrl, destPath, 'useCurl', true)`.

That is why documents store `ndic://…` rather than a URL: a pre-signed URL
expires, so the durable identifier is stored and the URL is minted at read
time.

**So the answer to "does the downloader need the dataset ID?" is: it depends
which half.** `getFile(downloadURL, downloadedFile)` needs nothing but a URL
and a destination — `useCurl` already defaults to true. But *obtaining a valid
URL* for an `ndic://` location needs the dataset ID, the file UID and cloud
authentication. Only the plain `url` type is fetchable from the URL alone.

Which makes DID-matlab's own `url` branch the one piece that is misplaced: a
plain `https://` location needs no dataset ID and no auth, yet DID reaches up
into `ndi.cloud.api.files.getFile` to fetch it. Giving DID-matlab a small
generic downloader for that branch would remove DID's dependency on NDI
entirely, leaving NDI responsible only for `ndicloud`, which it already
handles through the hook.

Such a downloader is not a one-liner. NDI's curl path carries four details
that were clearly learned the hard way, and all four would have to come with
it:

- `-H "Accept-Encoding: identity"` — the payloads are already-compressed
  archives, and letting the gateway compress them again produced corrupt files
  on both macOS and Linux (`websave` auto-decompresses and botches them).
- `-f` — so an HTTP error is a non-zero exit rather than a server error body
  written into the destination file.
- `assertSafeCurlArgs` (87 lines) — the URL is server-supplied and
  interpolated into a `system()` command inside double quotes, which in sh do
  **not** neutralise `` ` `` or `$`. This is a shell-injection guard.
- `ndi.common.systemCurlEnvPrefix` (68 lines) — resets `LD_LIBRARY_PATH` so
  the OS curl loads OS libraries rather than MATLAB's bundled ones, which a
  MATLAB upgrade can otherwise break.

**Do not copy the last two.** Duplicating a shell-injection guard across two
repositories means a fix to one silently fails to reach the other. Both are
generic utilities that belong in the lower layer: move them down into
DID-matlab (`did.file.assertSafeCurlArgs`, `did.common.systemCurlEnvPrefix`)
and have NDI call DID's copies, keeping only the cloud-API-specific parts.

**Still not verified: NDI-python.** The above was read from VH-Lab/ndi-matlab.
NDI-python is in the WalthamDataScience organization and was not examined, so
nothing here should be read as a claim about it. If it mirrors NDI-matlab's
design, DID-python needs the `custom_file_handler` parameter before NDI-python
has anywhere to hook.

### open_doc, fixed 2026-08-28

Three quiet bugs were fixed in `SQLiteDB.open_doc` along the way:

- `locations["location"]` was read directly, assuming a single dict. Documents
  may carry several locations per file — the shipped `demoFile.json` template
  lists a local path *and* a URL for each — so **any MATLAB-written document
  raised `TypeError: list indices must be integers`**. It now walks the
  locations in turn and returns the first that resolves, as MATLAB does.
- A URL was rebased against the database directory like a relative path,
  producing `/db/dir/https://example.org/data/thing.bin`.
- The resulting `Fileobj` was returned unopened. `Fileobj.fopen()` swallows
  the `OSError` and leaves `fid` None, so a caller that did not check `fid`
  read `b""` and saw no error — a missing file was indistinguishable from an
  empty one.

`open_doc` now raises `FileAccessError`, which carries MATLAB's identifier the
way `ValidationError` does and subclasses `FileNotFoundError` so existing
callers keep working:
`DID:SQLITEDB:FileRetrieval:UnsupportedType` when the only locations are
remote, `DID:SQLITEDB:open` otherwise.

Two things surfaced while fixing it, both now corrected:

- **`Fileobj.fread` returned different types on success and failure** — `bytes`
  when the file was open, the 2-tuple `(b"", 0)` when it was not. A caller
  doing `fread().decode()` got `AttributeError` on a tuple; one doing
  `data, count = fread()` unpacked correctly only when the read returned
  exactly two bytes. Now `b""` in both cases.
- **`tests/test_file_document.py` was asserting success on a file that could
  never be opened.** It wrote its fixture into the working directory while a
  relative location resolves against the *database* directory, and only
  checked `isinstance(file_obj, ReadOnlyFileobj)` — which the old silent
  behavior always satisfied. Fixture corrected and the test now reads the
  content back.

Coverage: `tests/test_open_doc_locations.py` (8 tests).

## File retrieval: the hook, implemented 2026-08-28

**Policy: only `ndic://` is supported. DID does not download anything itself,
in either language, and plain `http(s)` URLs are deliberately not handled.**
Retrieval is supplied by the caller through a handler, which is how NDI
already does it.

Both repos now behave the same way. `open_doc` walks the document's locations,
returns the first that resolves to a readable local file, and hands any remote
location to the handler:

| | MATLAB | Python |
|---|---|---|
| Parameter | `'customFileHandler', @fn` (name-value) | `custom_file_handler=fn` |
| Called as | `fn(destPath, sourcePath)` | `fn(dest_path, source_path)` |
| Contract | must produce a file at `destPath` | same, and it is checked |
| No handler, remote location | `DID:SQLITEDB:FileRetrieval:UnsupportedType` | same identifier, via `FileAccessError` |
| Handler failed | `…:CustomHandlerFailed` | same identifier |

**MATLAB change**: the `url` branch of `do_open_doc`, which called
`ndi.cloud.api.files.getFile` directly, was removed. It made DID depend on NDI
being on the path — a lower-level package reaching for a higher-level one —
for a case NDI already covers through the hook. Every non-`file` type now goes
to the handler.

**Python change**: `open_doc` gained the `custom_file_handler` parameter, so
NDI-python has somewhere to supply retrieval without DID acquiring a network
dependency.

### Two deviations that remain

- **~~No cache.~~ Closed 2026-08-29.** Python used to download into
  `PathConstants.temppath` and re-fetch on every open. It now does what MATLAB
  does: a retrieved file goes into the file cache under its uid, and a later
  open is served from there. See "The file cache".
- **~~`do_add_doc` still calls into NDI.~~ Closed 2026-08-30.** DID-matlab's
  *ingestion* path (`do_add_doc`, not `do_open_doc`) used to call
  `ndi.cloud.api.files.getFile` for a non-`file` location. Both languages now
  take a handler there instead — MATLAB's `options.customFileHandler`,
  Python's `custom_file_handler`, plumbed through `add_docs` — so DID
  downloads nothing on either path. It is rarely reached, because `ingest`
  defaults to 0 for `url` and `ndicloud` locations, which is presumably why it
  went unnoticed for so long. See "exist_doc and ingestion at add time".

### A hazard the tests caught

`temppath` persists between calls and a location's `uid` is unique per
document, not globally. The first implementation checked only whether a file
existed at the destination after the handler ran — so a leftover download from
an earlier open made a handler that produced *nothing* look like it had
succeeded, and would have served one document's bytes for another. The
destination is now cleared before the handler is invoked. Covered by
`test_a_stale_download_is_not_served_as_fresh` and
`test_a_fresh_download_replaces_a_stale_one`.

Coverage: `tests/test_open_doc_locations.py`, 23 tests. MATLAB has none for
this path.

## Files added by Python are invisible to MATLAB — FIXED 2026-08-29

The most serious symmetry break found in this audit, and it is one-directional,
which is why nothing caught it.

MATLAB's `do_add_doc` walks `doc_props.files.file_info` and inserts a row into
the **`files` table** for every location: `doc_idx, filename, uid,
orig_location, cached_location, type, parameters`. Python's `_do_add_doc`
writes only `docs.json_code` and the `branch_docs` link. It *creates* the
`files` table, with matching columns, and never inserts a row.

- **MATLAB → Python works.** MATLAB writes both `docs.json_code` and `files`;
  Python's `open_doc` reads locations out of the document JSON.
- **Python → MATLAB is broken.** MATLAB's `do_open_doc` selects from
  `docs, files`. On a Python-written database that join returns nothing, so
  **every file in the document is unreachable from MATLAB** — including a
  plain local file sitting on disk. It surfaces as `DID:SQLITEDB:open`, "The
  file … cannot be accessed".

Verified by adding a `demoFile` document with two local files through Python:
`docs` has 1 row, `files` has 0, Python's own `open_doc` succeeds, and
MATLAB's join returns nothing.

**Why the symmetry tests missed it**: the suite covered `buildDatabase` only,
whose demoA/demoB/demoC documents declare no `files` section, and it compares
database summaries, which carry no file information. Nothing in the suite had a
file, nothing called `open_doc`, and the compared summary had nowhere to show a
difference. A `fileDocument` symmetry pair was added alongside the fix.

**The fix turned out to be three changes, not one:**

1. `_do_add_doc` inserts one `files` row per location, with MATLAB's columns.
2. `open_doc` **reads** that table too, falling back to the document's own
   locations. This is the other half of the same gap: MATLAB deletes the
   original after ingesting, so for a MATLAB-written document the JSON location
   no longer exists and only the `files` rows can find the file. Ingested
   copies resolve at `<db dir>/files/<uid>`, which is how MATLAB derives
   `FileDir` and how it looks them up.
3. **`add_file` was under-ported**, and blocked both of the above. It recorded
   only the location string: no `uid`, no `location_type`, no `ingest` or
   `delete_original`, and it stored `locations` as a bare dict rather than the
   list MATLAB uses and the shipped `demoFile.json` carries. The missing `uid`
   was not cosmetic — it is the `UNIQUE` key of the `files` table, so two
   locations without one collapsed into a single row: **adding two files to a
   document produced exactly one row.**

`cached_location` is written when a location marked `ingest` is retrieved
through `custom_file_handler` (see "exist_doc and ingestion at add time"), and
is otherwise empty — including for a *local* location marked `ingest`, which
Python does not copy into `FileDir`. That last case is a real difference in
what the two write, not a gap in the row.

**Known remaining divergence**: MATLAB's `add_file` errors when the name is not
declared in the class's `file_list`; Python appends instead. Left alone as a
separate behavioral decision.

**Related, and partly closed**: Python ingests a *remote* location marked
`ingest` through `custom_file_handler` as of 2026-08-30. It still does not
ingest a *local* one: MATLAB copies such a location into `FileDir` and, when
`delete_original` is set, deletes the source. So for a local location `ingest`
and `delete_original` remain inert on the Python side. `delete_original` never
applies to the remote path — MATLAB explicitly skips deletion for a location
containing `://`, since a remote location is not ours to remove.

## exist_doc and ingestion at add time — ported 2026-08-30

Two file-service entry points existed in DID-matlab and not in DID-python
(DID-python#42). Both are ports: MATLAB defines the behaviour and is the
source of truth.

### `exist_doc(doc_id, filename)`

MATLAB's `did.database/exist_doc` validates the document id and delegates to
`sqlitedb/check_exist_doc`; both return `[tf, file_path]`. Python puts both
halves on `SQLiteDB.exist_doc`, exactly where `open_doc` lives, and returns
MATLAB's two outputs as a tuple:

```python
exists, file_path = db.exist_doc(doc_id, "filename1.ext")
```

Carried over from MATLAB: `doc_id` may be an id string **or** a document
object; only the first matching file is reported; an unknown document or an
unlisted filename is `False`, not an error; an empty filename **is** an error
(MATLAB's `DID:SQLITEDB:open`, Python's `ValueError` — it is a bad argument,
not a missing file).

Two things were decided deliberately:

- **`file_path` is `None`, not `""`, when the file does not exist.** MATLAB
  returns an empty char because that is its only empty string. Python has a
  value for "there is no path", and `None` cannot be mistaken for a usable
  relative path the way `""` can — `os.path.join(dir, "")` yields a directory.
- **It shares `open_doc`'s resolution** rather than growing a second copy of
  the `docs, files` join. `_locations_for_file` gathers the candidates and
  `_first_local_file` picks the first one on disk; `open_doc` calls both, so
  `exist_doc` is true *exactly* when `open_doc` would return a file with no
  `custom_file_handler`. `tests/test_exist_doc.py` asserts that agreement
  directly. MATLAB's `check_exist_doc` searches the same two roots as
  `do_open_doc` (`filecachepath/<uid>` and `FileDir/<uid>`) and stops there, so
  Python additionally reports true for a local `orig_location` that exists and
  MATLAB has not ingested — a superset, and the answer the caller wants, since
  MATLAB's own `do_open_doc` would open that file.

Neither language trusts a `files` row: one is inserted even when caching failed
and `cached_location` is empty, so the filesystem decides.

### `add_docs(..., custom_file_handler=...)`

MATLAB's `do_add_doc` walks every location whose `ingest` flag is set and puts
a copy at `<FileDir>/<uid>`. A `'file'` location is copied; anything else goes
to `options.customFileHandler(destPath, sourcePath)`. Python now does the same
for the non-local case, threaded `add_docs` → `_do_add_doc` →
`_populate_files` → `_ingest_location`, and records the result in
`files.cached_location` — which is where both languages then look for it.

The parameter is spelled `custom_file_handler`, matching `open_doc`'s
parameter for the identical contract rather than MATLAB's `customFileHandler`;
the same deliberate difference is recorded in the bridge.

**Failure warns, it does not raise.** A handler that throws, produces no file,
or was never supplied leaves `cached_location` empty and emits a warning — the
document is still added, `orig_location` is still recorded, and `open_doc` can
retrieve the file later with a handler. This follows MATLAB, whose
`warning('DID:SQLiteDB:add_doc', ...)` does exactly that: failing the whole add
would lose a document MATLAB is willing to store. The missing-handler warning
carries the same explanation `open_doc`'s error does, so the case is never
*silently* skipped.

One Python addition: an existing file at `<FileDir>/<uid>` is removed before
the handler runs, as `open_doc` already does for its download. Without it a
handler that produced nothing would be indistinguishable from one that
succeeded, and a stale file would be recorded as this document's ingested copy.

Coverage: `tests/test_exist_doc.py` (24 tests) and
`tests/test_add_docs_file_handler.py` (12 tests).

## The fileDocument symmetry pair, added 2026-08-29

`buildDatabase` exercises documents but never files. `fileDocument` exercises
files end to end, in both directions:

| | makeArtifacts | readArtifacts |
|---|---|---|
| Python | `tests/symmetry/make_artifacts/database/test_file_document.py` | `tests/symmetry/read_artifacts/database/test_file_document.py` |
| MATLAB | `tests_symmetry/+did/+symmetry/+makeArtifacts/+database/fileDocument.m` | `tests_symmetry/+did/+symmetry/+readArtifacts/+database/fileDocument.m` |

The maker builds a `demoFile` document with both declared files, each holding
ten deterministic bytes — file *i* holds `i*10 .. i*10+9` — writes a
`manifest.json` recording the document id and the expected bytes, and
self-checks by reading them back through `open_doc`. The reader, parameterized
over both artifact sources, opens each file and compares the bytes.

Two properties worth keeping if these are edited:

- **The reader skips rather than fails when the artifact is absent.** Each
  repository's symmetry job checks out the *other* repository's `main`, so a
  hard failure would mean neither half could land first. Skipping lets each
  side merge independently and the cross-language check become real once both
  are in.
- **The maker self-checks before writing the manifest.** A maker that silently
  produces a broken artifact turns into a *reader* failure in the other
  language, which is a much harder thing to diagnose.

### What can be verified without MATLAB

`tests/test_open_doc_locations.py:TestMatlabShapedDatabase` builds MATLAB's
on-disk shape by hand — files rows present, ingested copy at
`<db dir>/files/<uid>`, original deleted — and checks `open_doc` resolves it.
One of its cases deletes the `files` rows and asserts the same document becomes
unreadable, which is what demonstrates the table is load-bearing rather than
incidental.