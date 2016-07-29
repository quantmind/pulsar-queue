"""Tests task scheduling and execution"""
import os
import sys
import asyncio

from pulsar import send
from pulsar.apps import rpc

from pq import api


CODE_TEST = '''\
import time
def task_function(N = 10, lag = 0.1):
    time.sleep(lag)
    return N*N
'''

PATH = os.path.dirname(__file__)


def simple_task(self, value=0):
    return self.v0 + value


class TaskQueueBase:
    concurrency = 'process'
    # used for both keep-alive and timeout in JsonProxy
    # long enough to allow to wait for tasks
    rpc_timeout = 500
    tq_app = None
    rpc = None
    schedule_periodic = False
    message_serializer = 'json'

    @classmethod
    def name(cls):
        return cls.__name__.lower()

    @classmethod
    def rpc_name(cls):
        return 'rpc_%s' % cls.name()

    @classmethod
    async def setUpClass(cls):
        # The name of the task queue application
        params = cls.params()
        params.update(dict(
            wsgi=True,
            schedule_periodic=cls.schedule_periodic,
            rpc_bind='127.0.0.1:0',
            concurrency=cls.concurrency,
            message_serializer=cls.message_serializer,
            rpc_concurrency=cls.concurrency,
            rpc_keep_alive=cls.rpc_timeout
        ))
        pq = api.PulsarQueue(**params)
        cfgs = await pq.start()
        cls.tq_app = cfgs[0].app()
        cls.rpc = cfgs[1].app()
        # make sure the time out is high enough (bigger than test-timeout)
        cls.proxy = rpc.JsonProxy('http://%s:%s' % cls.rpc.cfg.addresses[0],
                                  timeout=cls.rpc_timeout)
        # Now flush the task queue
        cls.tq = await cls.tq_app.api().start()
        await cls.tq.flush_queues(*cls.queues())

    @classmethod
    def tearDownClass(cls):
        coros = [send('arbiter', 'kill_actor', a.name) for a in
                 (cls.tq_app, cls.rpc) if a is not None]
        return asyncio.gather(*coros)

    @classmethod
    def queues(cls):
        name = cls.name()
        return ['%s1' % name, '%s2' % name]

    @classmethod
    def params(cls):
        queues = cls.queues()
        return dict(
            name=cls.name(),
            config='tests.config',
            task_queues=queues,
            default_task_queue=queues[0]
        )


class TaskQueueApp(TaskQueueBase):

    def test_registry(self):
        self.assertTrue(isinstance(self.tq.registry, dict))
        regular = self.tq.registry.regular()
        periodic = self.tq.registry.periodic()
        self.assertTrue(regular)
        self.assertTrue(periodic)

    def test_producer(self):
        self.assertTrue(str(self.tq).startswith('task producer <'))
        self.assertEqual(self.tq.cfg.default_task_queue, '%s1' % self.name())

    def test_job_list(self):
        jobs = self.tq.job_list()
        self.assertTrue(jobs)
        self.assertTrue(isinstance(jobs, list))
        d = dict(jobs)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'], 'regular')

    async def test_simple_task(self):
        task = await self.tq.queue_task('addition', a=40, b=50)
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, 90)
        self.assertTrue(str(task).startswith('task.addition<'))
        self.assertTrue(task.done())

    async def test_simple_revoked(self):
        task = await self.tq.queue_task('addition', a=40, b=50, expiry=0)
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'REVOKED')
        self.assertFalse(task.result)

    async def test_info(self):
        task = await self.tq.queue_task('workerinfo')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertIsInstance(task.result, dict)
        self.assertEqual(len(task.result['queues']), 3)

    async def test_async_job(self):
        result = self.tq.queue_task('asynchronous', lag=2)
        self.assertIsInstance(result, asyncio.Future)
        task = await result
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertTrue(task.result >= 2)

    async def test_failure(self):
        task = await self.tq.queue_task('testperiodicerror', msg='testing')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'FAILURE')
        self.assertEqual(task.result, 'testing')
        self.assertTrue(task.stacktrace)

    async def test_execute_addition(self):
        future = self.tq.execute_task('addition', a=3, b=-4)
        self.assertIsInstance(future, api.TaskFuture)
        self.assertTrue(future.task_id)
        task = await future
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, -1)
        self.assertFalse(task.worker)
        self.assertFalse(task.queue)

    async def test_green_executor(self):
        task = await self.tq.queue_task('greenexecutor')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertIsInstance(task.result, dict)
        self.assertEqual(len(task.result['queues']), 3)

    async def test_local_queue(self):
        task = await self.tq.queue_task_local('testlocalqueue')
        self.assertIsInstance(task, api.Task)
        self.assertIsInstance(task.result, list)
        self.assertEqual(len(task.result), 3)
        self.assertEqual(task.result[0], self.tq.node_name)

    async def test_no_callback(self):
        task = await self.tq.queue_task('asynchronous', callback=False)
        self.assertTrue(task.id)
        self.assertEqual(task.status_string, 'QUEUED')
        self.assertTrue('ID=%s' % task.id in repr(task.done_callback))
        task = await task.done_callback
        self.assertEqual(task.status_string, 'SUCCESS')

    async def test_cpubound_task(self):
        task = await self.tq.queue_task('cpubound')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, ['OK', 2])

    async def __test_error_cpubound_task(self):
        task = await self.tq.queue_task('cpubound', error=True)
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'FAILURE')
        self.assertTrue(task.stacktrace)

    async def test_is_in_greenlet(self):
        task = await self.tq.queue_task('cpuboundwithasync')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, True)

    async def test_supports_asyncio(self):
        task = await self.tq.queue_task('cpuboundwithasync', asyncio=True)
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, False)

    async def test_big_log(self):
        # If this test fails, it is because the test runner will timeout on
        # this future, this is because the pipe fills up and blocks the
        # cpu bound task
        await self.tq.queue_task('cpuboundbiglog')

    async def test_execute_python_code(self):
        task = await self.tq.execute_task('execute.python',
                                          code='print("Hello World!")')
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, 'Hello World!')

    async def test_execute_python_script(self):
        script = os.path.join(PATH, 'example', 'executable.py')
        task = await self.tq.execute_task('execute.python.script',
                                          script=script)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, sys.executable)

    async def test_queue_size(self):
        code = "import time;time.sleep(1)"
        task = await asyncio.gather(
            self.tq.queue_task('execute.python', code=code, callback=False),
            self.tq.queue_task('execute.python', code=code, callback=False),
            self.tq.queue_task('execute.python', code=code, callback=False),
            self.tq.queue_task('execute.python', code=code, callback=False)
        )
        self.assertEqual(task[0].status_string, 'QUEUED')
        size = await self.tq.broker.size(task[0].queue)
        task = await asyncio.gather(
            task[0].done_callback,
            task[1].done_callback,
            task[2].done_callback,
            task[3].done_callback
        )
        self.assertEqual(task[0].status_string, 'SUCCESS')
        self.assertEqual(len(size), 1)
        self.assertTrue(size[0] > 0)

    async def test_lock(self):
        # make sure the lock is release (in case of errors)
        tasks = await asyncio.gather(
            self.tq.queue_task('notoverlap'),
            self.tq.queue_task('notoverlap'),
            self.tq.queue_task('notoverlap')
        )
        tasks = sorted(tasks, key=lambda task: task.result['start'])
        self.assertTrue(tasks[0].result['end'] < tasks[1].result['start'])
        self.assertTrue(tasks[1].result['end'] < tasks[2].result['start'])

    async def test_queue_from_task(self):
        task = await self.tq.queue_task('queue.from.task')
        self.assertEqual(task.status_string, 'SUCCESS')
        other_task = task.result
        self.assertEqual(other_task['from_task'], task.id)

    async def test_scrape(self):
        task = await self.tq.queue_task('scrape', url='https://www.bbc.co.uk/')
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertTrue(task.result)

    async def test_delay(self):
        task = await self.tq.queue_task('scrape',
                                        delay=2,
                                        url='https://www.bbc.co.uk/')
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.delay, 2)
        self.assertTrue(task.time_started - task.time_queued > 2)
        self.assertTrue(task.result)

    def _test_sync(self):
        loop = asyncio.new_event_loop()
        tasks = api.TaskApp(loop=loop, **self.params()).api()
        self.assertEqual(tasks._loop, loop)
        task = tasks.queue_task('scrape', url='https://github.com')
        self.assertIsInstance(task, asyncio.Future)
        self.assertTrue(task.task_id)
        task = task.wait()
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertFalse(tasks._loop.is_running())
        #
        task = tasks.queue_task('scrape', url='https://github.com',
                                callback=False)
        task = task.wait()
        self.assertEqual(task.status_string, 'QUEUED')
        self.assertTrue(task.done_callback)
        task = task.done_callback.wait()
        self.assertEqual(task.status_string, 'SUCCESS')

    # RPC
    async def test_rpc_job_list(self):
        data = await self.proxy.job_list()
        self.assertIsInstance(data, list)

    async def test_rpc_queue_task(self):
        task = await self.proxy.queue_task('cpubound')
        self.assertEqual(task['status'], 1)
