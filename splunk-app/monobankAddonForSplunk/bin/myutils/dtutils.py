from datetime import datetime, timedelta
import pytz


def dt2utc(dt, tz):
    """
    converts datetime to UTC
    :param dt: datetime
    :param tz: timezone
    :return: UTC datetime
    """
    try:
        loc_dt = tz.localize(dt)
    except ValueError:
        loc_dt = dt
    return tz.normalize(loc_dt).astimezone(pytz.utc)
