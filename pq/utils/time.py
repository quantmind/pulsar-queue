from datetime import datetime, timedelta


def timedelta_seconds(delta):
    return max(delta.total_seconds(), 0)


def get_time(expiry, start):
    if isinstance(expiry, timedelta):
        return (start + 86400 * expiry.days + expiry.seconds +
                0.000001 * expiry.microseconds)
    else:
        return start + expiry


def format_time(dt):
    dt = timestamp_to_datetime(dt)
    return dt.isoformat() if dt else '?'


def timestamp_to_datetime(timestamp):
    if isinstance(timestamp, (float, int)):
        timestamp = datetime.fromtimestamp(timestamp)
    return timestamp
