import sys
import time
import traceback
from asyncio import ensure_future
from asyncio.subprocess import Process

from pulsar import isawaitable, ImproperlyConfigured, CANCELLED_ERRORS
from pulsar.utils.system import json

from .task import TaskError, TaskTimeout
from . import models
from . import states
from .cpubound import StreamProtocol, PROCESS_FILE


class RemoteStackTrace(TaskError):
    pass


class ExecutorMixin:
    _concurrent_tasks = None

    async def _execute_task(self, worker, task):
        logger = self.logger
        task_id = task.id
        time_ended = time.time()
        JobClass = self.registry.get(task.name)
        try:
            if not JobClass:
                raise RuntimeError('%s not in registry' % task.name)
            if task.status > states.STARTED:
                expiry = task.expiry
                if expiry and time_ended > expiry:
                    raise TaskTimeout
                else:
                    kwargs = task.kwargs or {}
                    task.status = states.STARTED
                    task.time_started = time_ended
                    if worker:
                        task.worker = worker.aid
                    logger.info(task.lazy_info())
                    await self._pubsub.publish('started', task)
                    job = JobClass(self, task)
                    # This may block for a while
                    task.result = await self._consume(job, kwargs)
            else:
                raise TaskError('Invalid status %s' % task.status_string)
        except TaskTimeout:
            task.result = None
            task.status = states.REVOKED
            logger.info(task.lazy_info())
        except RemoteStackTrace:
            task.status = states.FAILURE
            logger.error(task.lazy_info())
        except Exception as exc:
            exc_info = sys.exc_info()
            task.result = str(exc)
            task.status = states.FAILURE
            task.stacktrace = traceback.format_tb(exc_info[2])
            task.exception = traceback.format_exception_only(
                exc_info[0], exc_info[1])[0]
            logger.exception(task.lazy_info())
        else:
            task.status = states.SUCCESS
            logger.info(task.lazy_info())
        #
        task.time_ended = time.time()
        if self._concurrent_tasks:
            self._concurrent_tasks.discard(task_id)
        await self._pubsub.publish('done', task)
        return task

    def _consume(self, job, kwargs):
        concurrency = job.get_concurrency()

        if concurrency == models.ASYNC_IO:
            result = job(**kwargs)
            assert isawaitable(result), "ASYNC_IO tasks not asynchronous"
            return result

        elif concurrency == models.GREEN_IO:
            return self.green_pool.submit(job, **kwargs)

        elif concurrency == models.THREAD_IO:
            return job._loop.run_in_executor(None, lambda: job(**kwargs))

        elif concurrency == models.CPUBOUND:
            return self._consume_in_subprocess(job, kwargs)

        else:
            raise ImproperlyConfigured('invalid concurrency "%d"' %
                                       concurrency)

    async def _consume_in_subprocess(self, job, kwargs):
        params = dict(self.json_params())
        loop = job._loop

        transport, protocol = await loop.subprocess_exec(
            lambda: StreamProtocol(job),
            sys.executable,
            PROCESS_FILE,
            json.dumps(sys.path),
            json.dumps(params),
            job.task.serialise())
        process = Process(transport, protocol, loop)
        await process.communicate()
        if job.task.stacktrace:
            raise RemoteStackTrace
        return job.task.result

    def json_params(self):
        for name, value in self.cfg.items():
            try:
                json.dumps(value)
            except Exception:
                continue
            yield name, value


class ConsumerMixin:
    """A mixin for consuming tasks from a distributed task queue.
    """
    queues = None

    @classmethod
    def __new__(cls, *args, **kwargs):
        o = super().__new__(cls)
        queues = [o.node_name]
        queues.extend(kwargs.get('queues') or ())
        o._queues = tuple(queues)
        o._processed = 0
        o._concurrent_tasks = set()
        return o

    def __repr__(self):
        return 'task consumer %s<%s>' % (self.queues, self.store.dns)

    @property
    def queues(self):
        '''List of task queues consumed by this task consumer
        '''
        return self._queues

    @property
    def num_concurrent_tasks(self):
        '''The number of concurrent_tasks
        '''
        return len(self._concurrent_tasks)

    def info(self):
        return {'concurrent': list(self._concurrent_tasks),
                'processed': self._processed,
                'queues': self._queues}

    def start(self, worker, queues=None):
        '''Starts consuming tasks
        '''
        self._pool_tasks(worker)
        self.logger.debug('%s started polling tasks', self)

    # #######################################################################
    # #    PRIVATE METHODS
    # #######################################################################
    def _pool_tasks(self, worker, next_time=None):
        assert self._queues, 'Task queues not specified, cannot pull tasks'
        if self._closing:
            if not self._concurrent_tasks:
                self.logger.warning(self._closing)
                worker._loop.stop()
        else:
            if worker.is_running() and not next_time:
                ensure_future(self._may_pool_task(worker), loop=worker._loop)
            else:
                next_time = next_time or 0
                worker._loop.call_later(next_time, self._pool_tasks, worker)

    async def _may_pool_task(self, worker):
        # Called in the ``worker`` event loop.
        #
        # It pools a new task if possible, and add it to the queue of
        # tasks consumed by the ``worker`` CPU-bound thread.'''
        next_time = None
        if worker.is_running():
            # executor = worker.executor()
            if self.num_concurrent_tasks < self.cfg.concurrent_tasks:
                max_tasks = self.cfg.max_requests
                if max_tasks and self._processed >= max_tasks:
                    self._closing = ('Processed %s tasks. Restarting.'
                                     % self._processed)

                if not self._closing:
                    try:
                        task = await self._pubsub.get_task(*self.queues)
                    except ConnectionRefusedError:
                        if worker.is_running():
                            self.logger.exception('Could not pool tasks')
                            next_time = 2
                    except CANCELLED_ERRORS:
                        self.logger.debug('stopped polling tasks')
                        raise
                    if task:  # Got a new task
                        self._processed += 1
                        self._concurrent_tasks.add(task.id)
                        ensure_future(self._execute_task(worker, task))
            else:
                self.logger.debug('%s concurrent requests. Cannot poll.',
                                  self.num_concurrent_tasks)
                next_time = 1
        self._pool_tasks(worker, next_time)
