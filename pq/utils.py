from datetime import datetime, timedelta

from . import states


def timedelta_seconds(delta):
    return max(delta.total_seconds(), 0)


def get_time(expiry, start):
    if isinstance(expiry, timedelta):
        return (start + 86400 * expiry.days + expiry.seconds +
                0.000001 * expiry.microseconds)
    else:
        return start + expiry


def format_time(dt):
    if isinstance(dt, (float, int)):
        dt = datetime.fromtimestamp(dt)
    return dt.isoformat() if dt else '?'


def nice_task_message(req, smart_time=None):
    smart_time = smart_time or format_time
    status = states.status_string(req.get('status'))
    user = req.get('user')
    ti = req.get('time_start', req.get('time_executed'))
    name = '%s (%s) ' % (req['name'], req['id'][:8])
    msg = '%s %s at %s' % (name, status, smart_time(ti))
    return '%s by %s' % (msg, user) if user else msg
