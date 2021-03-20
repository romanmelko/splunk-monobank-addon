"""
The strftime directive %z (with a lowercase z!) indicates time zone offset,
i.e. the signed time difference from UTC/GMT of the form +HHMM or -HHMM.
If you try to use this date format in Python 2 logging module you'll get the
unpleasant behaviour of silent failure.  Use LocalFormatter as a drop-in
replacement for built-in logging.Formatter if you intend to use a time zone
offset in your logging date format under Python 2.

See footnote https://docs.python.org/2/library/time.html#id2
"""
import logging
from datetime import datetime
from datetime import tzinfo

import pytz
import tzlocal


__version__ = '0.1'


class LocalFormatter(logging.Formatter):
    """a stdlib logging formatter that does the right thing with %z in the datefmt"""
    def __init__(self, fmt=None, datefmt=None, tz=None):
        if tz is None:
            self.tz = tzlocal.get_localzone()
        elif isinstance(tz, tzinfo):
            self.tz = tz
        else:
            self.tz = pytz.timezone(tz)
        super(LocalFormatter, self).__init__(fmt=fmt, datefmt=datefmt)

    def converter(self, timestamp):
        # logging.Formatter uses time.localtime here and returns a time.struct_time
        return datetime.fromtimestamp(timestamp, self.tz)

    def formatTime(self, record, datefmt=None):
        # Parent implementation expects the time record to be some sort of 9-item
        # sequence (e.g. time.struct_time).  Override impl to use datetime.strftime
        # instead of time.strftime to bypass this limitation.
        if datefmt is None:
            datefmt = '%Y-%m-%d %H:%M:%S%z'
        converted = self.converter(record.created)
        return converted.strftime(datefmt)


class ChicagoFormatter(LocalFormatter):

    def __init__(self, fmt=None, datefmt=None):
        super(ChicagoFormatter, self).__init__(fmt=fmt, datefmt=datefmt, tz='America/Chicago')
