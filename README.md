# DID-Python

DID-Python is a Python port of the [VH-Lab/DID-matlab](https://github.com/VH-lab/DID-matlab) project. It provides a flexible and extensible framework for managing data and metadata through a document-oriented interface, with support for multiple database backends.

## Features

*   **Document-Oriented:** A simple, intuitive interface for working with data as documents.
*   **Schema Validation:** Enforces data integrity using custom JSON-based schemas.
*   **Database Branching:** Manage different versions of your data with built-in branching support.
*   **SQLite Backend:** Includes a lightweight and easy-to-use implementation based on Python's built-in `sqlite3`.

## Setup

To get started with DID-Python, you will need to set up a Python virtual environment and install the required dependencies.

1.  **Create a virtual environment:**

    ```bash
    python -m venv venv
    ```

2.  **Activate the virtual environment:**

    *   On macOS and Linux:

        ```bash
        source venv/bin/activate
        ```

    *   On Windows:

        ```bash
        .\\venv\\Scripts\\activate
        ```

3.  **Install dependencies:**

    All required packages are listed in the `requirements.txt` file. Install them using pip:

    ```bash
    pip install -r requirements.txt
    ```

## Running the Test Suite

The project includes a comprehensive suite of unit tests. To run the full suite, execute the following command from the root of the project:

```bash
python -m unittest discover tests
```

## Documentation

For more detailed information about the project's architecture, API, and core concepts, please refer to the [full documentation](docs/index.md). The documentation is built using `mkdocs`.
