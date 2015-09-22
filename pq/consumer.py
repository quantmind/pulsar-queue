import time
import asyncio

from pulsar import is_async, ImproperlyConfigured, CANCELLED_ERRORS

from .producer import Task, TaskTimeout
from . import models
from . import states


class TaskContext(object):
    '''A context manager for consuming a task.
    '''
    def __init__(self, backend, worker, task, job):
        self.logger = worker.logger
        self.backend = backend
        self.worker = worker
        self.job = job
        self.task = task

    def __repr__(self):
        return 'consumer.%s' % self.task
    __str__ = __repr__

    @property
    def name(self):
        return self.name

    @property
    def _loop(self):
        return self.worker._loop


class ConsumerMixin:
    """A mixin for consuming tasks from a distributed task queue.
    """
    queue = None

    @classmethod
    def __new__(cls, *args, **kwargs):
        o = super().__new__(cls)
        o.processed = 0
        o.concurrent_tasks = set()
        return o

    @property
    def num_concurrent_tasks(self):
        '''The number of :attr:`concurrent_tasks`.

        This number is never greater than the :attr:`backlog` attribute.
        '''
        return len(self.concurrent_tasks)

    def info(self):
        return {'concurrent': list(self.concurrent_tasks),
                'processed': self.processed,
                'queue': self.queue}

    def start(self, worker):
        '''Starts consuming tasks
        '''
        self.may_pool_task(worker)
        self.logger.debug('%s started polling tasks', self)

    def close(self):
        '''Close this :class:`.TaskBackend`.

        Invoked by the :class:`.Actor` when stopping.
        '''
        if not self._closing:
            self._closing = 'closing'

    # #######################################################################
    # #    PRIVATE METHODS
    # #######################################################################
    def may_pool_task(self, worker, next_time=None):
        assert self.queue, 'Task queue not specified, cannot pull tasks'
        if self._closing:
            if not self.num_concurrent_tasks:
                self.logger.warning(self._closing)
                worker._loop.stop()
        else:
            if worker.is_running() and not next_time:
                asyncio.async(self._may_pool_task(worker), loop=worker._loop)
            else:
                next_time = next_time or 0
                worker._loop.call_later(next_time, self.may_pool_task, worker)

    def _may_pool_task(self, worker):
        # Called in the ``worker`` event loop.
        #
        # It pools a new task if possible, and add it to the queue of
        # tasks consumed by the ``worker`` CPU-bound thread.'''
        next_time = None
        if worker.is_running():
            # executor = worker.executor()
            if self.num_concurrent_tasks < self.cfg.concurrent_tasks:
                max_tasks = self.cfg.max_requests
                if max_tasks and self.processed >= max_tasks:
                    self._closing = ('Processed %s tasks. Restarting.'
                                     % self.processed)

                if not self._closing:
                    try:
                        task = yield from self._get_task()
                    except ConnectionRefusedError:
                        if worker.is_running():
                            raise
                    except CANCELLED_ERRORS:
                        self.logger.debug('stopped polling tasks')
                        raise
                    if task:    # Got a new task
                        self.processed += 1
                        self.concurrent_tasks.add(task.id)
                        asyncio.async(self._execute_task(worker, task))
            else:
                self.logger.debug('%s concurrent requests. Cannot poll.',
                                  self.num_concurrent_tasks)
                next_time = 1
        self.may_pool_task(worker, next_time)

    def _get_task(self):
        '''Asynchronously retrieve a :class:`Task` from  the :attr:`queue`

        :return: a :class:`.Task` or ``None``.
        '''
        client = self.store.client()
        queue_stask = yield from client.execute('brpop',
                                                self.queue,
                                                self.cfg.task_pool_timeout)
        if queue_stask:
            _, stask = queue_stask
            return Task.load(stask, self.cfg.params.get('TASK_SERIALISATION'))

    def _execute_task(self, worker, task):
        logger = worker.logger
        task_id = task.id
        time_ended = time.time()
        job = self.registry.get(task.name)
        context = TaskContext(self, worker, task, job)
        task_info = task.lazy_info()
        try:
            if not context.job:
                raise RuntimeError('%s not in registry' % task_info)
            if task.status > states.STARTED:
                expiry = task.expiry
                if expiry and time_ended > expiry:
                    raise TaskTimeout
                else:
                    logger.info('starting %s', task_info)
                    kwargs = task.kwargs or {}
                    task.status = states.STARTED
                    task.time_started = time_ended,
                    task.worker = worker.aid
                    yield from self._publish('started', task)
                    # This may block for a while

                    result = yield from self._consume(context, kwargs)
                    if is_async(result):
                        task.result = yield from result
                    else:
                        task.result = result
                    task.status = states.SUCCESS
            else:
                logger.error('invalid status for %s', task_info)
        except TaskTimeout:
            logger.warning('%s timed-out', task_info)
            task.result = None
            task.status = states.REVOKED
        except Exception as exc:
            logger.exception('failure in %s', task_info)
            task.result = str(exc)
            task.status = states.FAILURE
        #
        task.time_ended = time.time()
        self.concurrent_tasks.discard(task_id)
        #
        logger.info('finished %s', task_info)
        yield from self._publish('done', task)

    def _consume(self, context, kwargs):
        job = context.job
        concurrency = job.concurrency or models.THREAD_IO

        if concurrency == models.ASYNC_IO:
            result = job(context, **kwargs)
            assert is_async(result), "ASYNC_IO tasks not asynchronous"
            return result

        elif concurrency == models.GREEN_IO:
            return self._green_pool.submit(job, context, **kwargs)

        elif concurrency == models.THREAD_IO:
            loop = context._loop
            return loop.run_in_executor(None, job, context, **kwargs)

        elif concurrency == models.CPUBOUND:
            return self._consume_in_subprocess()

        else:
            raise ImproperlyConfigured('invalid concurrency')
