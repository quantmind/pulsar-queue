from pulsar import PulsarException
from pulsar.utils.log import LazyString

from ..utils.serializers import Message
from . import states


__all__ = ['TaskError',
           'TaskNotAvailable',
           'TaskTimeout',
           'Task']


class TaskError(PulsarException):
    status = states.FAILURE


class TaskNotAvailable(TaskError):
    MESSAGE = 'Task {0} is not registered'

    def __init__(self, task_name):
        self.task_name = task_name
        super().__init__(self.MESSAGE.format(task_name))


class TaskTimeout(TaskError):
    pass


class Task(Message):
    '''A class containing task execution data
    '''
    def __init__(self, id=None, name=None, time_queued=None,
                 time_started=None, time_ended=None,
                 result=None, exception=None, stacktrace=None, worker=None,
                 timeout=None, status=None, kwargs=None, queue=None,
                 retry=None, delay=None, meta=None, run_info=None, **kw):
        self.__dict__.update(kw)
        self.id = id
        self.name = name
        self.queue = queue
        self.time_queued = time_queued
        self.time_started = time_started
        self.time_ended = time_ended
        self.result = result
        self.exception = exception
        self.stacktrace = stacktrace
        self.worker = worker
        self.timeout = timeout
        self.status = status
        self.retry = retry or 1
        self.delay = delay
        self.kwargs = kwargs
        self.run_info = run_info if run_info is not None else {}
        self.meta = meta if meta is not None else {}
        self.meta.update(kw)

    def __repr__(self):
        return self.info()
    __str__ = __repr__

    @classmethod
    def consumer(cls):
        return 'tasks'

    @property
    def full_name(self):
        return 'task.%s' % self.name

    @property
    def status_string(self):
        '''A string representation of :attr:`status` code
        '''
        return states.status_string(self.status)

    @property
    def expiry(self):
        if self.timeout:
            return self.time_queued + (self.delay or 0) + self.timeout

    def done(self):
        '''Return ``True`` if the :class:`Task` has finshed.

        Its status is one of :ref:`READY_STATES <task-ready-state>`.
        '''
        return self.status in states.READY_STATES

    def info(self):
        '''Information string about the task
        '''
        return '%s<%s><%s>' % (self.full_name, self.id, self.status_string)

    def lazy_info(self):
        '''Lazy information string (useful for logging)
        '''
        return LazyString(self.info)
