# Welcome to DID-Python

This project is a Python port of the VH-Lab/DID-matlab project, a flexible framework for managing data and metadata across different database backends. DID-Python provides a simple, document-oriented interface for interacting with complex datasets.

## Core Concepts

The DID-Python library is built around two fundamental classes:

*   **`did.document`**: Represents a single, self-describing data record. Each document has a unique ID and a set of properties defined by a JSON schema. Documents can also have dependencies on other documents, forming a directed acyclic graph (DAG) of relationships.
*   **`did.database`**: An abstract base class that defines the interface for storing and retrieving documents. This project includes a concrete implementation, `did.implementations.SQLiteDB`, which uses the built-in `sqlite3` library as a backend.

## Getting Started

To begin using DID-Python, you will need to set up a Python virtual environment and install the necessary dependencies. Full instructions can be found in the project's `README.md` file.
