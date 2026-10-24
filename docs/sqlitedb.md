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

### Where to keep the database file

Keep a database that is in use **outside** folders that a sync or backup client
manages, such as Dropbox, OneDrive, iCloud Drive or Google Drive. The `files/`
directory next to it, where ingested files are stored, needs the same care.

SQLite works through the file it opened, not through its name, and sync
clients change files behind its back. DID catches one of the ways they do it;
it cannot see the others:

- **Replacing the file** with a downloaded version. DID detects this, both
  before a write starts and just before it commits, and replays the write
  against the new file. A replacement in the instant between that last check
  and the commit is not caught.
- **Overwriting the file in place**, or uploading it half-written while a
  write is under way. The other copy then holds a damaged or out-of-date
  database.
- **Conflicted copies.** When two machines change the same database, the client
  keeps both as separate files, and the changes in one never reach the other.

To share a database or keep a backup, close it first (`db.close()`, in every
process that has it open), then copy the `.sqlite` file and its `files/`
directory together. Copy them to a synced folder if you like. Just don't open
a database from there while the client is running.
