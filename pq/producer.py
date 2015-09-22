import json
import time
import asyncio

from pulsar import EventHandler, ImproperlyConfigured, PulsarException
from pulsar.apps.data import create_store
from pulsar.utils.log import LazyString
from pulsar.utils.string import gen_unique_id, to_string

from .pubsub import PubSubMixin
from .models import RegistryMixin
from .utils import get_time
from . import states


def all_queues(cfg):
    yield cfg.default_task_queue
    for queue in cfg.task_queues:
        if queue != cfg.default_task_queue:
            yield queue


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
    '''A data :class:`.Model` containing task execution data.
    '''
    time_started = None
    time_ended = None
    result = None

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
        return self.info
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

    def serialise(self, method=None):
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

    def status_string(self):
        '''A string representation of :attr:`status` code
        '''
        return states.status_string(self.status)

    def info(self):
        return '%s<%s>' % (self.full_name, self.id)

    def lazy_info(self):
        return LazyString(self.info)

    def channel(self, name):
        '''Given an event ``name`` returns the corresponding channel name.

        The event ``name`` is one of ``task_queued``, ``task_started``
        or ``task_done``
        '''
        assert self.queue
        return '%s_%s' % (self.queue, name)


class TaskProducer(EventHandler, PubSubMixin, RegistryMixin):
    """Produce tasks by queuing them
    """
    MANY_TIMES_EVENTS = ('task_queued', 'task_started', 'task_done')

    def __init__(self, cfg, queue=None, logger=None):
        self.store = create_store(cfg.data_store)
        super().__init__(self.store._loop)
        self.cfg = cfg
        self._logger = logger
        self._queue = queue
        self._callbacks = {}
        self._pubsub = self._get_pubsub()
        self._closing = False
        self.logger.debug('created %s', self)

    def __repr__(self):
        if self.cfg.schedule_periodic:
            return 'task scheduler <%s>' % self.store.dns
        elif self.queue:
            return 'task consumer %s<%s>' % (self.queue, self.store.dns)
        else:
            return 'task producer <%s>' % self.store.dns
    __str__ = __repr__

    @property
    def queue(self):
        return self._queue

    def flush(self):
        client = self.store.client()
        if self.queue:
            return client.execute('del', self.queue)
        else:
            pipe = client.pipeline()
            for queue in all_queues(self.cfg):
                pipe.execute('del', queue)
            return pipe.commit()

    def close(self):
        '''Close this :class:`.TaskBackend`.

        Invoked by the :class:`.Actor` when stopping.
        '''
        if not self._closing:
            self._closing = 'closing'

    def queue_task(self, jobname, meta_params=None, expiry=None, **kwargs):
        '''Try to queue a new :ref:`Task`.

        This method returns a :class:`.Future` which results in the
        task ``id`` created. If ``jobname`` is not a valid
        :attr:`.Job.name`, a ``TaskNotAvailable`` exception occurs.

        :param jobname: the name of a :class:`.Job`
            registered with the :class:`.TaskQueue` application.
        :param meta_params: Additional parameters to be passed to the
            :class:`Task` constructor (not its callable function).
        :param expiry: optional expiry timestamp to override the default
            expiry of a task.
        :param kwargs: optional dictionary used for the key-valued arguments
            in the task callable.
        :return: a :class:`.Future` resulting in a task once finished or
            Nothing
        '''
        if self._closing:
            self.logger.warning('Cannot queue task, task backend closing')
            return
        if jobname in self.registry:
            job = self.registry[jobname]
            task_id = gen_unique_id()
            queued = time.time()
            if expiry is not None:
                expiry = get_time(expiry, queued)
            elif job.timeout:
                expiry = get_time(job.timeout, queued)
            meta_params = meta_params or {}
            task = Task(task_id,
                        name=job.name,
                        queue=job.queue or self.cfg.default_task_queue,
                        time_queued=queued,
                        expiry=expiry,
                        kwargs=kwargs,
                        status=states.QUEUED,
                        **meta_params)
            callback = asyncio.Future(loop=self._loop)
            self._callbacks[task_id] = callback
            asyncio.async(self._queue_task(task), loop=self._loop)
            return callback
        else:
            raise TaskNotAvailable(jobname)

    def _queue_task(self, task):
        '''Asynchronously queue a task
        '''
        stask = self._serialise(task)
        yield from self.store.client().lpush(task.queue, stask)
        yield from self._publish('queued', task)
        scheduled = self.entries.get(task.name)
        if scheduled:
            scheduled.next()
        self.logger.debug('queued %s in "%s"', task.lazy_info(), task.queue)

    def _task_done_callback(self, task, exc=None):
        done = self._callbacks.pop(task.id, None)
        if done:
            done.set_result(task)
