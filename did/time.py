# astropy is used over datetime because it supports leap seconds
from astropy.time import Time, TimeISO
from .exception import InvalidTimeFormat

from datetime import datetime

def current_time():
    time = Time.now()
    time.format = 'isot'
    return str(time)

def check_time_format(thing):
    try:
        datetime.fromisoformat(thing)
        return True
    except ValueError as error:
        raise InvalidTimeFormat('Expected [ISOT format](https://docs.astropy.org/en/stable/api/astropy.time.TimeISOT.html#astropy.time.TimeISOT).')
