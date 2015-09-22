import time
import asyncio
from datetime import timedelta

import pq


class RunPyCode(pq.Job):
    '''execute python code in *code*. There must be a *task_function*
function defined which accept key-valued parameters only.'''
    timeout = timedelta(seconds=60)

    def __call__(self, consumer, code, **kwargs):
        code_local = compile(code, '<string>', 'exec')
        ns = {}
        exec(code_local, ns)
        func = ns['task_function']
        return func(**kwargs)


class Addition(pq.Job):
    timeout = timedelta(seconds=60)

    def __call__(self, consumer, a=0, b=0):
        return a + b


class Asynchronous(pq.Job):
    concurrency = pq.ASYNC_IO

    def __call__(self, consumer, lag=1):
        start = time.time()
        yield from asyncio.sleep(lag)
        return time.time() - start


class NotOverLap(pq.Job):

    def __call__(self, consumer, lag=1):
        with consumer.lock():
            start = time.time()
            yield from asyncio.sleep(lag)
            return time.time() - start


class CheckWorker(pq.Job):

    def __call__(self, consumer):
        backend = consumer.backend
        return {'tasks': list(backend.concurrent_tasks)}
