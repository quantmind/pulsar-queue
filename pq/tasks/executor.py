import sys
import time
import traceback
from asyncio import wait_for
from asyncio import CancelledError, TimeoutError
from asyncio.subprocess import Process

from pulsar.utils.system import json

from .task import TaskError, TaskTimeout
from . import states
from ..cpubound import StreamProtocol, PROCESS_FILE
from ..utils.exc import string_exception
from ..utils import concurrency


consumer_event = 'consumer_status'


class RemoteStackTrace(TaskError):
    pass


class TooManyTasksForJob(TaskError):
    status = states.REVOKED


class ExecutorMixin:
    # Mixin for both TaskConsumer and TaskProducer
    #
    # The TaskProducer can execute a task inline, while the consumer executes
    # task from a task queue via the ConsumerMixin
    #
    async def _execute_task(self, task, worker=None):
        # Function executing a task
        #
        # - If the stat_time is greater than task.expiry Revoke the Task
        # - If the task has a delay not yet reached
        #   - queue the task at the right time
        #   - otherwise proceed to next
        # - Set status to STARTED and consume the task
        logger = self.logger
        task_id = task.id
        time_ended = time.time()
        job = None
        JobClass = self.registry.get(task.name)

        try:
            if not JobClass:
                raise RuntimeError('%s not in registry' % task.name)

            if task.status > states.STARTED:
                queued = task.time_queued
                timeout = task.timeout
                delay = task.delay or 0
                start = queued + delay

                if delay:  # Task with delay
                    gap = start - time_ended
                    if gap > 0:
                        self._loop.call_later(gap, self._queue_again, task)
                        if worker:
                            self._concurrent_tasks.pop(task_id, None)
                        return task

                if timeout:  # Handle timeout
                    timeout = timeout + start - time_ended
                    if timeout <= 0:
                        raise TaskTimeout

                if worker:
                    concurrent = await self.broker.incr(JobClass.name)

                job = JobClass(self, task)

                if job.max_concurrency and concurrent > job.max_concurrency:
                    raise TooManyTasksForJob('max concurrency %d reached',
                                             job.max_concurrency)

                kwargs = task.kwargs or {}
                task.status = states.STARTED
                task.time_started = time_ended
                if worker:
                    task.worker = worker.aid
                logger.info(task.lazy_info())
                await self.pubsub.publish('started', task)
                future = self._consume(job, kwargs)
                #
                # record future for cancellation
                if worker:
                    self._concurrent_tasks[task_id].future = future
                #
                # This may block until timeout
                task.result = await wait_for(future, timeout)
            else:
                raise TaskError('Invalid status %s' % task.status_string)

        except (CancelledError, TimeoutError, TaskTimeout):
            task.result = None
            task.status = states.REVOKED
            logger.error(task.lazy_info())
        except RemoteStackTrace:
            task.status = states.FAILURE
            logger.error(task.lazy_info())
        except TaskError as exc:
            task.result = string_exception(exc)
            task.status = exc.status
            logger.error(task.lazy_info())
        except Exception as exc:
            exc_info = sys.exc_info()
            task.result = string_exception(exc)
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
        if worker:
            self._concurrent_tasks.pop(task_id, None)

        await self.pubsub.publish('done', task)

        if job:
            if worker:
                await self.broker.decr(job.name)
            if self._should_retry(job):
                await self._requeue_task(job)

        return task

    def _consume(self, job, kwargs):
        model = job.get_concurrency()

        if model == concurrency.THREAD_IO:
            return job._loop.run_in_executor(None, lambda: job(**kwargs))

        elif model == concurrency.CPUBOUND:
            return self._consume_in_subprocess(job, kwargs)

        else:
            return self.backend.green_pool.submit(job, **kwargs)

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

    def _should_retry(self, job):
        return (job.task.status != states.SUCCESS and
                job.task.queue and
                job.max_retries and
                job.task.retry < job.max_retries)

    def _requeue_task(self, job):
        task = job.task
        meta_params = task.meta.copy()
        meta_params['retry'] = task.retry + 1
        return job.queue(
            job.name,
            callback=False,
            meta_params=meta_params,
            queue=task.queue,
            delay=job.retry_delay,
            **task.kwargs
        )

    def _queue_again(self, task):
        self.broker.queue(task, False)

    def json_params(self):
        for name, value in self.cfg.items():
            try:
                json.dumps(value)
            except Exception:
                continue
            yield name, value
