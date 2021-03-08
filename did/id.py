import random, struct
from datetime import datetime as dt

class DIDId:
    """
    This class creates and stores globally unique IDs. The ID is based on both the
	current time and a random number (see did.unique_id). Therefore, the IDs are 
	globally unique and also sortable (alphanumerically) by the time of creation
	(which is in Universal Controlled Time (Leap Seconds), UTC). 
    """
    
    def __init__(self, id=None):
        if id:
            self.id = id
        else:
            self._id = DIDId._new_unique_id()
    
    @property
    def id(self):
        """
        the unique identifier of the did object

        return: id of the DIDId object
        rtype: str
        """
        return self._id

    @id.setter
    def id(self, id):
        #TODO perform id validation
        self._id = id
    
    def __str__(self):
        return self.id

    def __repr__(self):
        return 'DIDId({})'.format(self._id)

    @staticmethod
    def _new_unique_id():
        """
        Generates a unique ID str based on the current time and a random
		number. It is a hexidecimal representation of the serial date number in
		UTC Leap Seconds time. The serial date number is the number of days since January 0, 0000 at 0:00:00.
		The integer portion of the date is the whole number of days and the fractional part of the date number
		is the fraction of days.

        return: unique id for DIDDocument
        rtype: str
        """
        date = dt.now()
        integral_part = date.toordinal()
        fractional_part = (date - date.fromordinal(integral_part)).total_seconds() / (24 * 60 * 60)
        serial_date_number = struct.unpack('!q', struct.pack('!d', integral_part + fractional_part))[0]
        hex_serial_date_number = format(serial_date_number, 'x')
        
        random_number = abs(random.random() + random.randint(-32727, 32727))
        hex_random_number = format(struct.unpack('!q', struct.pack('!d', random_number))[0], 'x')
        return "{}_{}".format(hex_serial_date_number, hex_random_number)