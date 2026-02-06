# `did.implementations.sqlitedb`

The `did.implementations.sqlitedb` module provides the `SQLiteDB` class, which is a concrete implementation of the `did.database.Database` interface that uses SQLite as its backend.

## `SQLiteDB` Class

The `SQLiteDB` class provides a file-based database that is suitable for local use. It uses Python's built-in `sqlite3` module to interact with the database file.

### Usage

To create a new `SQLiteDB` object, simply provide a filename for the database file:

```python
from did.implementations.sqlitedb import SQLiteDB

db = SQLiteDB('mydatabase.sqlite')
```

If the database file does not exist, it will be created automatically.

### Methods

The `SQLiteDB` class implements all of the abstract methods of the `Database` class, as well as the public methods for interacting with the database.