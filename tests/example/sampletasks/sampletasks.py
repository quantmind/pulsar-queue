import time
import asyncio
from datetime import timedelta

import greenlet

from pq import api


class TestError(Exception):
    pass


class RunPyCode(api.Job):
    '''execute python code in *code*. There must be a *task_function*
function defined which accept key-valued parameters only.'''
    timeout = timedelta(seconds=60)

    def __call__(self, code=None, **kwargs):
        code_local = compile(code, '<string>', 'exec')
        ns = {}
        exec(code_local, ns)
        func = ns['task_function']
        return func(**kwargs)


class Addition(api.Job):
    timeout = timedelta(seconds=60)

    def __call__(self, a=0, b=0):
        return a + b


class Asynchronous(api.Job):
    concurrency = api.ASYNC_IO

    async def __call__(self, lag=1):
        start = time.time()
        await asyncio.sleep(lag)
        return time.time() - start


@api.job(concurrency=api.ASYNC_IO)
async def notoverlap(self, lag=1):
    async with self.lock():
        start = time.time()
        await asyncio.sleep(lag)
        return {
            'start': start,
            'end': time.time()
        }


@api.job(concurrency=api.ASYNC_IO)
async def queue_from_task(self):
    task = await self.queue_task('asynchronous')
    return task.tojson()


class WorkerInfo(api.Job):
    concurrency = api.GREEN_IO

    def __call__(self):
        return self.backend.info()


class GreenExecutor(api.Job):
    concurrency = api.GREEN_IO

    def __call__(self):
        return self.run_in_executor(self.backend.info)


class CpuBound(api.Job):
    concurrency = api.CPUBOUND

    def __call__(self, error=False):
        self.logger.info('Testing CpuBound concurrency')
        self.logger.warning('Sleeping for 2 seconds')
        time.sleep(1)
        if error:
            raise TestError('just a test')
        return ['OK', 2]


@api.job()
def testlocalqueue(self):
    return self.backend.queues()


class CpuBoundWithAsync(api.Job):
    concurrency = api.CPUBOUND

    def __call__(self, asyncio=False):
        if asyncio:
            return self.asyncio()
        else:
            return self.greenlet_info()

    def greenlet_info(self):
        return greenlet.getcurrent().parent is not None

    async def asyncio(self):
        await asyncio.sleep(1)
        return self.greenlet_info()


class CpuBoundBigLog(api.Job):
    concurrency = api.CPUBOUND

    def __call__(self):
        # Log more date then the pipe buffer, as logs are send through the pipe
        for i in range(1024):
            self.backend.logger.info('*'*1024)
