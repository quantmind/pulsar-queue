import time
from datetime import datetime, timedelta

from pulsar.utils.log import lazyproperty

from .utils import timedelta_seconds


class SchedulerMixin:
    '''Schedule new tasks
    '''

    @classmethod
    def __new__(cls, *args, **kwargs):
        o = super().__new__(cls)
        o.next_run = time.time()
        return o

    @lazyproperty
    def entries(self):
        return self._setup_schedule()

    def tick(self, now=None):
        # Run a tick, that is one iteration of the scheduler.
        if self._closing or not self.cfg.schedule_periodic:
            return
        remaining_times = []
        for entry in self.entries.values():
            is_due, next_time_to_run = entry.is_due(now=now)
            #
            # Task is now due
            if is_due:
                self.queue_task(entry.name)
                entry.next()

            if next_time_to_run:
                remaining_times.append(next_time_to_run)
        self.next_run = now or time.time()
        if remaining_times:
            self.next_run += min(remaining_times)

    def next_scheduled(self, jobnames=None):
        if not self.cfg.schedule_periodic:
            return
        if jobnames:
            entries = (self.entries.get(name, None) for name in jobnames)
        else:
            entries = self.entries.values()
        next_entry = None
        next_time = None
        for entry in entries:
            if entry is None:
                continue
            is_due, next_time_to_run = entry.is_due()
            if is_due:
                next_time = 0
                next_entry = entry
                break
            elif next_time_to_run is not None:
                if next_time is None or next_time_to_run < next_time:
                    next_time = next_time_to_run
                    next_entry = entry
        if next_entry:
            return (next_entry.name, max(next_time, 0))
        else:
            return (jobnames, None)

    def _setup_schedule(self):
        entries = {}
        if not self.cfg.schedule_periodic:
            return entries
        for name, t in self.registry.filter_types('periodic'):
            every = t.run_every
            if isinstance(every, int):
                every = timedelta(seconds=every)
            if not isinstance(every, timedelta):
                raise ValueError('Schedule %s is not a timedelta' % every)
            entries[name] = SchedulerEntry(name, every, t.anchor)
        return entries


class SchedulerEntry(object):
    '''A class used as a schedule entry by the :class:`.TaskBackend`.

    .. attribute:: name

        Task name

    .. attribute:: run_every

        Interval in seconds

    .. attribute:: anchor

        Datetime anchor

    .. attribute:: last_run_at

        last run datetime

    .. attribute:: total_run_count

        Total number of times this periodic task has been executed by the
        :class:`.TaskBackend`.
    '''

    def __init__(self, name, run_every, anchor=None):
        self.name = name
        self.run_every = run_every
        self.anchor = anchor
        self.last_run_at = datetime.now()
        self.total_run_count = 0

    def __repr__(self):
        return self.name

    __str__ = __repr__

    @property
    def scheduled_last_run_at(self):
        '''The scheduled last run datetime.

        This is different from :attr:`last_run_at` only when
        :attr:`anchor` is set.
        '''
        last_run_at = self.last_run_at
        anchor = self.anchor
        if last_run_at and anchor:
            run_every = self.run_every
            times = int(timedelta_seconds(last_run_at - anchor) /
                        timedelta_seconds(run_every))
            if times:
                anchor += times * run_every
                while anchor <= last_run_at:
                    anchor += run_every
                while anchor > last_run_at:
                    anchor -= run_every
                self.anchor = anchor
            return anchor
        else:
            return last_run_at

    def next(self, now=None):
        '''Increase the :attr:`total_run_count` attribute by one and set the
        value of :attr:`last_run_at` to ``now``.
        '''
        self.last_run_at = now or datetime.now()
        self.total_run_count += 1

    def is_due(self, now=None):
        '''Returns tuple of two items ``(is_due, next_time_to_run)``,
        where next time to run is in seconds.

        See :meth:`unuk.contrib.tasks.models.PeriodicTask.is_due`
        for more information.
        '''
        last_run_at = self.scheduled_last_run_at
        now = now or datetime.now()
        rem_delta = last_run_at + self.run_every - now
        rem = timedelta_seconds(rem_delta)
        if rem == 0:
            return True, timedelta_seconds(self.run_every)
        return False, rem
