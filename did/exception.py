class NoTransactionError(Exception): pass

class NoWorkingSnapshotError(Exception): pass

class NoChangesToSave(Exception): pass

class SnapshotIntegrityError(Exception): pass

class InvalidTimeFormat(Exception): pass

class IntegrityError(Exception): pass