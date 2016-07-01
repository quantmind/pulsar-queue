from pulsar import ImproperlyConfigured, PulsarException
from pulsar.utils.system import json
from pulsar.utils.log import LazyString
from pulsar.utils.string import to_string

from . import states


__all__ = ['TaskError',
           'TaskNotAvailable',
           'TaskTimeout',
           'Task']


class TaskError(PulsarException):
    pass


class TaskNotAvailable(TaskError):
    MESSAGE = 'Task {0} is not registered'

    def __init__(self, task_name):
        self.task_name = task_name
        super().__init__(self.MESSAGE.format(task_name))


class TaskTimeout(TaskError):
    pass


class Task:
    '''A class containing task execution data
    '''
    time_started = None
    time_ended = None
    result = None
    exception = None
    stacktrace = None
    worker = None

    def __init__(self, id=None, name=None, time_queued=None,
                 expiry=None, status=None, kwargs=None, queue=None,
                 **kw):
        self.id = id
        self.name = name
        self.queue = queue
        self.time_queued = time_queued
        self.expiry = expiry
        self.status = status
        self.kwargs = kwargs
        self.__dict__.update(kw)

    def __repr__(self):
        return self.info()
    __str__ = __repr__

    @classmethod
    def load(cls, data, method=None):
        method = method or 'json'
        if method == 'json':
            return cls(**json.loads(to_string(data)))
        else:
            raise ImproperlyConfigured('Unknown serialisation "%s"' % method)

    @property
    def full_name(self):
        return 'task.%s' % self.name

    @property
    def status_string(self):
        '''A string representation of :attr:`status` code
        '''
        return states.status_string(self.status)

    def serialise(self, method=None):
        '''Serialise this task using the serialisation ``method``
        '''
        method = method or 'json'
        if method == 'json':
            return json.dumps(self.__dict__)
        else:
            raise ImproperlyConfigured('Unknown serialisation "%s"' % method)

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
