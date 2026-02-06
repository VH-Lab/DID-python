# Welcome to DID-Python

This is a Python port of the VH-Lab/DID-matlab project.

## Overview

The `did` package provides a framework for managing and interacting with a document-based database. It includes a database interface, document objects, and various utilities for handling data.

## Key Modules

*   **`did.database`**: Defines the abstract base class for database implementations.
*   **`did.document`**: Defines the `Document` class, which represents a single document in the database.
*   **`did.implementations.sqlitedb`**: A concrete implementation of the `did.database` interface using SQLite.
*   **`did.query`**: Provides a `Query` class for constructing database searches.
*   **`did.fun`**: Contains functions for graph analysis and plotting of document dependencies.

## Getting Started

To get started, please see the [Setup](../README.md) instructions.