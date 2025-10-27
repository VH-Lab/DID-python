# Agent Guidelines for DID-Python

This document provides instructions and guidelines for AI agents working on the DID-Python repository.

## Project Overview
The goal of this project is to create a complete and robust Python port of the [VH-Lab/DID-matlab](https://github.com/VH-lab/DID-matlab) project. The library provides a document-oriented interface for managing data and metadata with support for database branching and a SQLite backend.

## Core Technologies
- **Backend:** Python's built-in `sqlite3` module. Do not use external database drivers unless explicitly instructed.
- **Dependencies:** All project dependencies are managed in `requirements.txt`. If you add a new dependency, you must add it to this file and run `pip install -r requirements.txt`.
- **Graph Operations:** The `networkx` library is used for graph manipulation, particularly in the test suite for handling branch structures.
- **Documentation:** Project documentation is built using `mkdocs`.

## Development Workflow & Verification
1.  **Environment Setup:** Before starting, ensure you are in a Python virtual environment and have installed all dependencies with `pip install -r requirements.txt`.
2.  **Run All Tests:** The primary verification command for this repository is `python -m unittest discover tests`. You **must** run this command after making changes to ensure no regressions have been introduced. All tests must be passing before you submit your work.

## Architectural Guidelines
- **Custom Schema Validation:** This project uses a **custom schema format**, not standard JSON Schema. The validation logic is implemented in the `did.database.Database` class. Do not attempt to use standard JSON Schema validators.
- **Schema File Location:** All document and schema definition files are located in the `schemas/database_documents` and `schemas/database_schema` directories. The logic for finding these files is in `did/document.py`.
- **Path Constants:** The project uses special placeholders in its schema files (e.g., `$DIDDOCUMENT_EX1`). These are resolved at runtime using the `DEFINITIONS` dictionary in `did/common/path_constants.py`. If you encounter a `FileNotFoundError` related to schemas, verify that all necessary paths are defined here.
- **Application Data:** The library uses the `appdirs` package to manage user-specific application data. Do not hardcode paths to user directories (e.g., `~/Documents`).
- **Database Commits:** When working with the `SQLiteDB` implementation, you **must** call `conn.commit()` after any data modification (e.g., INSERT, UPDATE, DELETE) to avoid `database is locked` errors.

## Testing Strategy
- **Test Isolation:** When creating new test files, ensure each test class uses a unique database filename and that the file is deleted in the `setUp` method to prevent tests from interfering with each other.
- **Non-Deterministic Helpers:** The `make_doc_tree_invalid` function in `tests/helpers.py` is non-deterministic. If a test is failing intermittently, check if this helper is the cause. Logic has been added to this function to force the creation of certain document types (e.g., those with dependencies) when specific modifiers are used. Follow this pattern if you extend the test suite.
- **Porting from Matlab:** When porting code from the Matlab version:
    - Convert CamelCase variable and function names to snake_case.
    - Python class names should mimic the namespace structure of the Matlab version.
    - Port Matlab `unittest` objects to Python `unittest` objects.
    - Port Matlab test helpers to Python test helpers.
