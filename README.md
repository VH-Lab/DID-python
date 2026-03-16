# DID.py

This is a Python port of the VH-Lab/DID-matlab project.

## Introduction

The `did` library provides a framework for managing and querying data that is organized into documents and branches. It is designed to be a flexible and extensible system for data management and analysis.

## Key Features

*   **Document-oriented data model:** Data is stored in documents, which are flexible, JSON-like structures.
*   **Branching and versioning:** The library supports branching, allowing you to create and manage different versions of your data.
*   **Powerful querying:** The library provides a rich query language for retrieving documents from the database.
*   **Extensible architecture:** The library is designed to be extensible, allowing you to add new functionality and data types.

## Getting Started

### Prerequisites

*   Python 3.10 or later
*   `pip` for installing packages

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/DID-python.git
    cd DID-python
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install the package and dependencies:**
    ```bash
    pip install -e ".[dev]"
    ```

### Running the Tests

You can run the tests using either `pytest` (if you installed the development dependencies) or the standard `unittest` module.

**Run all tests (unit + symmetry):**
```bash
pytest
```

**Run only the unit tests (excluding symmetry tests):**
```bash
pytest tests/ --ignore=tests/symmetry
```

**Run only the symmetry tests:**
```bash
pytest -m symmetry
```

**Run only the makeArtifact symmetry tests** (generate cross-language artifacts):
```bash
pytest -m make_artifacts
```

**Run only the readArtifact symmetry tests** (validate artifacts from Python and/or MATLAB):
```bash
pytest -m read_artifacts
```

**Using unittest (unit tests only):**
```bash
python -m unittest discover tests
```

#### Symmetry Tests

The `tests/symmetry/` directory contains cross-language symmetry tests that verify
DID databases created in Python can be read by MATLAB and vice versa:

*   **`make_artifacts/`** — Creates a DID database with multiple branches and
    documents, then writes the database file and JSON summary artifacts to a
    well-known temporary directory
    (`<tempdir>/DID/symmetryTest/pythonArtifacts/`).
*   **`read_artifacts/`** — Reads artifacts produced by either the Python or
    MATLAB test suite, re-summarizes the live database, and compares the
    result against the saved summary. Tests are parameterized over
    `matlabArtifacts` and `pythonArtifacts` and skip gracefully when
    artifacts from a given source are not available.

The CI workflow runs the full cross-language cycle:
1. MATLAB `makeArtifact` tests create artifacts
2. Python `makeArtifact` and `readArtifact` tests run (reading MATLAB artifacts)
3. MATLAB `readArtifact` tests run (reading Python artifacts)

## Documentation

The documentation for this project is built using `mkdocs`. To build the documentation, run the following command:

```bash
mkdocs build
```

The documentation will be generated in the `site` directory. You can then open the `site/index.html` file in your web browser to view the documentation.
