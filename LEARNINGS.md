# Learnings from the DID-matlab to Python Port

This document captures key architectural decisions, technical details, and project conventions discovered and established during the porting process.

## Project Goal & Core Technologies
- The primary goal is to port the VH-Lab/DID-matlab project to Python.
- The Python port uses the built-in `sqlite3` module, replacing the `mksqlite` dependency from the Matlab version.
- Project dependencies are managed in `requirements.txt` and installed via `pip`.
- The project is structured as a standard Python package and can be installed with `pip install -e .`.

## Coding Conventions
- **Naming:** Matlab's CamelCase function and variable names should be converted to snake_case in Python. Class names should mimic the namespace structure of the Matlab version.
- **Dependencies:** Do not use `doc.set_properties` to set `depends_on` fields. Use the dedicated dependency methods of the `did.document.py` class.

## Architecture & Implementation Notes
- **Custom Schema:** The project uses a custom schema format, not standard JSON Schema. A custom validation engine was built into `did.database.Database` to handle this.
- **Schema Location:** The project is self-contained. All necessary schema files are located in the `schemas/database_documents` and `schemas/database_schema` subdirectories.
- **Path Resolution:** Schema files contain placeholders (e.g., `$DIDDOCUMENT_EX1`, `$DIDSCHEMA_EX1`). These are resolved at runtime using definitions in `did/common/path_constants.py`.
- **Application Data:** The library uses the `appdirs` package to store application data (like file caches and preferences) in platform-appropriate user directories, avoiding hardcoded paths.
- **`BinaryTable`:** The `did.file.BinaryTable` class, which originally used a custom binary format, has been re-implemented to use `sqlite3` as its backend for robustness and simplicity.

## Testing
- **Test Runner:** The full test suite can be run from the project root using the command: `python -m unittest discover tests`.
- **Test Isolation:** Each test class should use a unique database filename (e.g., `test_name.sqlite`) and ensure it is deleted during `setUp` to prevent state from leaking between tests.
- **Database Commits:** All database operations in the `SQLiteDB` implementation that modify data (e.g., INSERT, DELETE) must be followed by a `conn.commit()` to prevent `database is locked` errors.
- **Non-Deterministic Tests:** The `make_doc_tree_invalid` test helper is non-deterministic. When writing tests that rely on it, logic may be needed to enforce the creation of specific document types to ensure validation logic is not accidentally skipped.
- **Graph Structures:** The `networkx` library is used in tests to represent and manipulate complex graph structures, such as the database branch tree in `TestBranch`.

## Repository Management
- **Documentation:** The project uses `mkdocs` for documentation.
- **README:** The `README.md` file should contain clear, up-to-date instructions on how to set up the environment and run the tests.
- **Git Ignore:** Do not check in test artifacts. The `.gitignore` file is configured to ignore temporary database files (`*.sqlite`), the `files/` cache directory, and common Python artifacts.
