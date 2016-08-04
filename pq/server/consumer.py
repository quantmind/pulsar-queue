import sys
import time
import traceback
from asyncio import ensure_future, Future
from asyncio.subprocess import Process

from pulsar import CANCELLED_ERRORS
from pulsar.utils.system import json

from ..tasks.task import TaskError, TaskTimeout
from ..tasks import models
from ..tasks import states
from ..cpubound import StreamProtocol, PROCESS_FILE
from .pubsub import backoff, RECONNECT_LAG


consumer_event = 'consumer_status'


class RemoteStackTrace(TaskError):
    pass


class ExecutorMixin:
    _concurrent_tasks = None

    async def _execute_task(self, task, worker=None):
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

                if task.delay:  # Task with delay
                    start_time = task.time_queued + task.delay
                    gap = start_time - time_ended
                    if gap > 0:
                        self._loop.call_later(gap, self._queue_again, task)
                        if self._concurrent_tasks:
                            self._concurrent_tasks.discard(task_id)
                        return task

                kwargs = task.kwargs or {}
                task.status = states.STARTED
                task.time_started = time_ended
                if worker:
                    task.worker = worker.aid
                logger.info(task.lazy_info())
                await self.pubsub.publish('started', task)
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
        await self.pubsub.publish('done', task)
        return task

    def _consume(self, job, kwargs):
        concurrency = job.get_concurrency()

        if concurrency == models.THREAD_IO:
            return job._loop.run_in_executor(None, lambda: job(**kwargs))

        elif concurrency == models.CPUBOUND:
            return self._consume_in_subprocess(job, kwargs)

        else:
            return self.green_pool.submit(job, **kwargs)

    async def _consume_in_subprocess(self, job, kwargs):
        params = dict(self.json_params())
        loop = job._loop

        transport, protocol = await loop.subprocess_exec(
            lambda: StreamProtocol(job),
            sys.executable,
            PROCESS_FILE,
            json.dumps(sys.path),
            json.dumps(params),
            json.dumps(job.task.tojson()))
        process = Process(transport, protocol, loop)
        await process.communicate()
        if job.task.stacktrace:
            raise RemoteStackTrace
        return job.task.result

    def _queue_again(self, task):
        self.broker.queue(task, False)

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
    @classmethod
    def __new__(cls, *args, **kwargs):
        o = super().__new__(cls)
        o._processed = 0
        o._next_time = 1
        o._closing_waiter = None
        o._concurrent_tasks = set()
        return o

    def __repr__(self):
        return 'task consumer <%s>' % self.broker

    def queues(self):
        '''List of task queues consumed by this task consumer
        '''
        return self.manager.queues()

    @property
    def num_concurrent_tasks(self):
        '''The number of concurrent_tasks
        '''
        return len(self._concurrent_tasks)

    def info(self):
        return {'node': self.node_name,
                'concurrent': list(self._concurrent_tasks),
                'processed': self._processed,
                'queues': self.queues()}

    async def start(self, worker):
        '''Starts consuming tasks
        '''
        await self.pubsub.start()
        self._poll_tasks(worker)
        self.logger.warning('%s started polling tasks', self)
        return self

    def closing(self):
        return self._closing_waiter is not None

    def close(self, msg=None):
        if not self.closing():
            self._closing_waiter = Future(loop=self._loop)
            self.logger.warning(msg)
        return self._closing_waiter

    # #######################################################################
    # #    PRIVATE METHODS
    # #######################################################################
    def _poll_tasks(self, worker, next_time=None):
        if self.closing() and not self._concurrent_tasks:
            self._do_close(worker)
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
        next_time = None
        if worker.is_running():
            # executor = worker.executor()
            if self.num_concurrent_tasks < self.cfg.concurrent_tasks:
                max_tasks = self.cfg.max_requests
                if max_tasks and self._processed >= max_tasks:
                    self.close(
                        'Processed %s tasks. Stop polling tasks.'
                        % self._processed
                    )

                if not self.closing():
                    try:
                        task = await self.broker.get_task(*self.queues())
                    except ConnectionRefusedError:
                        if self.broker.connection_error:
                            self._next_time = backoff(self._next_time)
                        else:
                            self._next_time = RECONNECT_LAG
                            self.broker.connection_error = True
                        next_time = self._next_time
                        task = None
                        if worker.is_running():
                            self.logger.critical(
                                '%s cannot pool tasks - '
                                'connection error - try again in %s seconds',
                                self.broker,
                                next_time
                            )
                    except CANCELLED_ERRORS:
                        self.logger.debug('stopped polling tasks')
                        raise
                    else:
                        self.broker.connection_ok()
                    if task:  # Got a new task
                        self._processed += 1
                        self._concurrent_tasks.add(task.id)
                        ensure_future(self._execute_task(task, worker))
                    await self._broadcast(worker)
            else:
                self.logger.debug('%s concurrent requests. Cannot poll.',
                                  self.num_concurrent_tasks)
                self._next_time = 1
                next_time = self._next_time
                await self._broadcast(worker)

        self._poll_tasks(worker, next_time)

    def _broadcast(self, worker):
        info = self.info()
        info['worker'] = worker.aid
        info['time'] = time.time()
        return self.pubsub.publish(consumer_event, info)

    def _do_close(self, worker):
        self.logger.warning('Closing %s', self)
        self.manager.close()
        self._closing_waiter.set_result(True)
        if not worker.is_monitor():
            self._loop.call_later(1, self._loop.stop)
