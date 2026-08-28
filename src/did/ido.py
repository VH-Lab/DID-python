import random
import re
import struct
from datetime import datetime, timezone

# MATLAB's datenum counts days from "January 0, 0000"; Python's
# date.toordinal() counts from 0001-01-01 == 1. The offset between them.
_DATENUM_ORDINAL_OFFSET = 366


def _num2hex(value):
    """The IEEE-754 big-endian bit pattern of a double, as 16 lowercase hex digits.

    Equivalent to MATLAB's num2hex for a double.
    """
    return struct.pack(">d", float(value)).hex()


def _serial_date_number(moment=None):
    """MATLAB's datenum for a given instant: days (and fraction) since Jan 0, 0000."""
    if moment is None:
        moment = datetime.now(timezone.utc)
    seconds_into_day = (
        moment.hour * 3600
        + moment.minute * 60
        + moment.second
        + moment.microsecond / 1e6
    )
    return moment.toordinal() + _DATENUM_ORDINAL_OFFSET + seconds_into_day / 86400.0


class IDO:
    """Identifier object; creates globally unique IDs for a DID database.

    The ID is a hexadecimal representation of the serial date number followed
    by an underscore and a hexadecimal random number, so IDs are globally
    unique and also sortable alphanumerically by creation time. Mirrors MATLAB
    did.ido.
    """

    def __init__(self, id_value=None):
        if id_value:
            # Store what the caller gave us, as MATLAB's did.ido does. Use
            # is_valid() to check an id rather than relying on construction.
            self.identifier = id_value
        else:
            self.identifier = self.unique_id()

    def id(self):
        return self.identifier

    @staticmethod
    def unique_id():
        """Generate a unique ID based on the current time and a random number.

        ``ID = [num2hex(serial_date_number) '_' num2hex(rand)]`` -- 16 hex
        digits, an underscore, and 16 more. Mirrors MATLAB
        ``did.ido.unique_id``.

        Deviation: MATLAB derives the serial date number from ``clock``, which
        is local time; this uses UTC, as did.ido's own documentation specifies.
        IDs are only ever compared for equality across the two languages, never
        parsed back into a time, and UTC keeps them sortable across machines in
        different time zones.
        """
        random_number = random.random() + random.randint(-32727, 32727)
        return f"{_num2hex(_serial_date_number())}_{_num2hex(random_number)}"

    @staticmethod
    def is_valid(id_value):
        """Is this a structurally valid DID identifier?

        A valid ID is 16 hexadecimal digits (0-9, a-f), an underscore, and 16
        more. Mirrors MATLAB ``did.ido.isvalid``.
        """
        return bool(re.fullmatch(r"[0-9a-f]{16}_[0-9a-f]{16}", str(id_value)))
