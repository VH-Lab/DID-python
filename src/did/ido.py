import uuid
import re

class IDO:
    def __init__(self, id_value=None):
        if id_value and self.is_valid(id_value):
            self.identifier = id_value
        else:
            self.identifier = self.unique_id()

    def id(self):
        return self.identifier

    @staticmethod
    def unique_id():
        """
        Generates a unique ID.
        """
        # Using UUID4 for simplicity and robustness
        return str(uuid.uuid4())

    @staticmethod
    def is_valid(id_value):
        """
        Checks if a unique ID is valid.
        """
        # A simple regex to check for UUID format
        pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z', re.I)
        return bool(pattern.match(str(id_value)))