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

To run the test suite, use the following command:

```bash
python -m unittest discover tests
```

This will discover and run all the tests in the `tests` directory.

## Documentation

The documentation for this project is built using `mkdocs`. To build the documentation, run the following command:

```bash
mkdocs build
```

The documentation will be generated in the `site` directory. You can then open the `site/index.html` file in your web browser to view the documentation.
