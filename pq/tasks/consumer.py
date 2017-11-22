import time
from math import exp
from uuid import uuid4

from pulsar.async.access import ensure_future, CANCELLED_ERRORS
from pulsar.apps.data.channels import backoff, RECONNECT_LAG

from .executor import ExecutorMixin
from .scheduler import SchedulerMixin
from . import states
from .task import Task, TaskNotAvailable
from .models import RegistryMixin
from .rpc import TasksRpc
from ..consumer import ConsumerAPI


FACTOR = exp(1) - 2


def poll_time(a, b, x, lag=0):
    a = max(a, 0)   # 0 minimum pool gap
    b = max(a, b)   # b cannot be less than a
    return max(a + (b-a) * (exp(x) - x - 1)/FACTOR - lag, 0)


class Tasks(RegistryMixin, ExecutorMixin, SchedulerMixin, ConsumerAPI):
    """A Consumer for processing tasks
    """
    def __init__(self, backend):
        super().__init__(backend)
        self._processed = 0
        self._next_time = 1
        self._concurrent_tasks = {}

    def start(self, worker):
        self._polling_tasks = True
        self._poll_tasks(worker)
        self.logger.warning('%s started polling tasks', self)

    async def register(self):
        await self.channels.register(Task.type, 'done', self._task_done)

    def queues(self):
        '''List of task queues consumed by this task consumer
        '''
        return self.manager.queues()

    @property
    def max_concurrent_tasks(self):
        return self.cfg.concurrent_tasks(self.cfg)

    @property
    def num_concurrent_tasks(self):
        '''The number of concurrent_tasks
        '''
        return len(self._concurrent_tasks)

    def info(self):
        return {
            'max_concurrent_tasks': self.max_concurrent_tasks,
            'concurrent_tasks': list(self._concurrent_tasks),
            'processed': self._processed,
            'pulltime': self._next_time,
            'queues': self.queues()
        }

    def rpc(self):
        return TasksRpc()

    # API
    def queue(self, jobname, callback=True, **kwargs):
        '''Try to queue a new :task

        :param callback: when true (default) return a future called back once
            the task done, otherwise it is called back once the task is queued.
        :return: a :class:`.Future` resulting in a task once finished or
            Nothing
        '''
        task = self._create_task(jobname, **kwargs)
        if task:
            future = self.broker.queue(task, callback)
            if self._loop.is_running():
                return self.backend.green_pool.wait(future)
            else:
                return future

    def queue_local(self, jobname, **kwargs):
        kwargs['queue'] = self.backend.node_name
        return self.queue(jobname, **kwargs)

    def execute(self, jobname, **kwargs):
        '''Execute a task immediately
        '''
        if isinstance(jobname, Task):
            return self._execute_task(jobname)
        else:
            kwargs['queue'] = False
            kwargs['callback'] = True
            return self.queue(jobname, **kwargs)

    def flush_queues(self, *queues):
        return self.broker.flush_queues(*queues)

    def gen_unique_id(self):
        return uuid4().hex

    # #######################################################################
    # #    PRIVATE METHODS
    # #######################################################################
    def _task_done(self, channel, event, task):
        done = self.backend.broker.queued_messages.pop(task.id, None)
        if done:
            done.set_result(task)

    def _poll_tasks(self, worker, next_time=None):
        if self.closing() and not self._concurrent_tasks:
            self.do_close()
        elif worker.is_running() and not next_time:
            ensure_future(self._may_poll_task(worker), loop=worker._loop)
        else:
            next_time = next_time or 0
            worker._loop.call_later(next_time, self._poll_tasks, worker)

    async def _may_poll_task(self, worker):
        # Called in the ``worker`` event loop.
        #
        # It pools a new task if possible, and add it to the queue of
        # tasks consumed by the ``worker`` CPU-bound thread.'''
        task = None
        next_time = None
        lag = 0
        if worker.is_running():
            loop = worker._loop

            if self.num_concurrent_tasks < self.max_concurrent_tasks:
                max_tasks = self.cfg.max_requests
                if max_tasks and self._processed >= max_tasks:
                    self.backend.close(
                        'Processed %s tasks. Stop polling tasks.'
                        % self._processed
                    )

                if not self.closing():
                    try:
                        t0 = loop.time()
                        task = await self.broker.get_message(*self.queues())
                        lag = loop.time() - t0
                    except ConnectionError:
                        if self.broker.connection_error:
                            next_time = backoff(self._next_time)
                        else:
                            next_time = RECONNECT_LAG
                            self.broker.connection_error = True
                        if worker.is_running():
                            self.logger.critical(
                                '%s cannot pool messages - '
                                'connection error - try again in %s seconds',
                                self.broker,
                                next_time
                            )
                    except CANCELLED_ERRORS:
                        self.logger.debug('stopped polling messages')
                        raise
                    except Exception:
                        if worker.is_running():
                            self.logger.exception('server exception')
                    else:
                        self.broker.connection_ok()
                    if task:  # Got a new task
                        self._processed += 1
                        self._concurrent_tasks[task.id] = TaskExecutor(task)
                        ensure_future(self._execute_task(task, worker))
            else:
                self.logger.debug('%s concurrent messages. Cannot poll.',
                                  self.max_concurrent_tasks)

            if next_time is None:
                next_time = poll_time(
                    self.cfg.task_pool_timeout,
                    self.cfg.task_pool_timeout_max,
                    self.num_concurrent_tasks/self.max_concurrent_tasks,
                    lag
                )
            self._next_time = next_time

        self._poll_tasks(worker, next_time)

    def _create_task(self, jobname, meta_params=None, timeout=None, queue=True,
                     delay=None, **kwargs):
        '''Try to queue a new :ref:`Task`.

        This method returns a :class:`.Future` which results in the
        task ``id`` created. If ``jobname`` is not a valid
        :attr:`.Job.name`, a ``TaskNotAvailable`` exception occurs.

        :param jobname: the name of a :class:`.Job`
            registered with the :class:`.TaskQueue` application.
        :param meta_params: Additional parameters to be passed to the
            :class:`Task` constructor (not its callable function).
        :param timeout: optional expiry timestamp to override the default
            timeout of a task.
        :param kwargs: optional dictionary used for the key-valued arguments
            in the task callable.
        :return: a :class:`.Future` resulting in a task once finished or
            Nothing
        '''
        if self.closing():
            self.logger.warning('Cannot queue task, task backend closing')
            return
        if jobname in self.registry:
            job = self.registry[jobname]
            task_id = self.gen_unique_id()
            queued = time.time()
            timeout = timeout or job.timeout
            meta_params = meta_params or {}
            if queue is not False:
                if queue is True:
                    queue = job.default_queue or self.cfg.default_task_queue
            else:
                queue = None
            return Task(task_id,
                        name=job.name,
                        queue=queue,
                        time_queued=queued,
                        timeout=timeout,
                        kwargs=kwargs,
                        status=states.QUEUED,
                        delay=delay,
                        **meta_params)
        else:
            raise TaskNotAvailable(jobname)


class TaskExecutor:

    def __init__(self, task):
        self.task = task
        self.future = None
