import time
import logging

from pulsar.apps.data import create_store
from pulsar.utils.string import gen_unique_id

from .pubsub import PubSub
from .task import Task, TaskNotAvailable
from .models import RegistryMixin
from .utils import get_time
from . import states


class TaskProducer(RegistryMixin):
    """Produce tasks by queuing them
    """
    def __init__(self, cfg, logger=None, app=None, **kw):
        self.store = create_store(cfg.data_store)
        self.cfg = cfg
        self.app = app
        self.logger = logger or logging.getLogger('pulsar.queue')
        self._closing = False
        self._pubsub = PubSub(self)
        self.logger.debug('created %s', self)

    def __repr__(self):
        if self.cfg.schedule_periodic:
            return 'task scheduler <%s>' % self.store.dns
        else:
            return 'task producer <%s>' % self.store.dns

    def __str__(self):
        return repr(self)

    @property
    def _loop(self):
        return self.store._loop

    def ready(self):
        return self._pubsub._subscribed

    def flush_queues(self, *queues):
        return self._pubsub.flush_queues(*queues)

    def on_events(self, callback):
        self._pubsub.on_events(callback)

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
            queue = job.queue or self.cfg.default_task_queue
            if self.cfg.task_queue_prefix:
                queue = '%s_%s' % (self.cfg.task_queue_prefix, queue)
            task = Task(task_id,
                        name=job.name,
                        queue=queue,
                        time_queued=queued,
                        expiry=expiry,
                        kwargs=kwargs,
                        status=states.QUEUED,
                        **meta_params)
            return self._pubsub.queue(task)
        else:
            raise TaskNotAvailable(jobname)
