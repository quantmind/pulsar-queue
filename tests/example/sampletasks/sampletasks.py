import time
import asyncio
from datetime import timedelta

import pq


class TestError(Exception):
    pass


class RunPyCode(pq.Job):
    '''execute python code in *code*. There must be a *task_function*
function defined which accept key-valued parameters only.'''
    timeout = timedelta(seconds=60)

    def __call__(self, code=None, **kwargs):
        code_local = compile(code, '<string>', 'exec')
        ns = {}
        exec(code_local, ns)
        func = ns['task_function']
        return func(**kwargs)


class Addition(pq.Job):
    timeout = timedelta(seconds=60)

    def __call__(self, a=0, b=0):
        return a + b


class Asynchronous(pq.Job):
    concurrency = pq.ASYNC_IO

    def __call__(self, lag=1):
        start = time.time()
        yield from asyncio.sleep(lag)
        return time.time() - start


class NotOverLap(pq.Job):

    def __call__(self, lag=1):
        with self.lock():
            start = time.time()
            yield from asyncio.sleep(lag)
            return time.time() - start


class WorkerInfo(pq.Job):

    def __call__(self):
        return self.backend.info()


class CpuBound(pq.Job):
    concurrency = pq.CPUBOUND

    def __call__(self, error=False):
        self.logger.info('Testing CpuBound concurrency')
        self.logger.warning('Testing warning')
        if error:
            raise TestError('just a test')
        time.sleep(2)
        return 'OK'
