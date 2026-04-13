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

As of 2026-04-13, the repositories are **in sync** for all core functionality.

### Recently resolved (no Python changes needed)
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
- **sqlitedb**: MATLAB replaced `websave` with `ndi.cloud.api.files.getFile`
  for URL downloads. This is MATLAB-ecosystem-specific; Python uses its own
  download mechanism. **Not applicable to Python.**

## Not Yet Ported from MATLAB

These MATLAB features do not yet have Python counterparts:

| MATLAB feature | Bridge file | Priority |
|---|---|---|
| `database.freeze_branch` | bridge.yaml | Low |
| `database.is_branch_editable` | bridge.yaml | Low |
| `database.display_branches` | bridge.yaml | Low |
| `database.exist_doc` | bridge.yaml | Medium |
| `database.close_doc` | bridge.yaml | Low |
| `document.validate` | bridge.yaml | Medium |
| `document.dependency_value_n` | bridge.yaml | Low |
| `document.add_dependency_value_n` | bridge.yaml | Low |
| `document.remove_dependency_value_n` | bridge.yaml | Low |
| `binaryTable` write methods | bridge_file.yaml | Medium |
