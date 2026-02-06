# `did.document`

The `did.document` module provides the `Document` class, which represents a single document in the database.

## `Document` Class

The `Document` class is a container for the data and metadata associated with a document. It is created with a document type, which determines the schema that the document must adhere to.

### Key Methods

*   `__init__(self, document_type='base', **kwargs)`: Creates a new `Document` object.
*   `id()`: Returns the unique identifier of the document.
*   `set_properties(**kwargs)`: Sets the properties of the document.
*   `dependency_value(dependency_name, error_if_not_found=True)`: Returns the value of a dependency.
*   `set_dependency_value(dependency_name, value, error_if_not_found=True)`: Sets the value of a dependency.

### Document Properties

The `document_properties` attribute of a `Document` object is a dictionary that contains the data and metadata for the document. The structure of this dictionary is defined by the document's schema.