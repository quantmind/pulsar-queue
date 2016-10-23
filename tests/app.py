"""Tests task scheduling and execution"""
import os
import sys
import asyncio
import threading

from pulsar import send, create_future
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
    # used for both keep-alive and timeout in JsonProxy
    # long enough to allow to wait for tasks
    rpc_timeout = 500
    max_requests = 0
    concurrent_tasks = 5
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
        params.update(
            wsgi=True,
            schedule_periodic=cls.schedule_periodic,
            rpc_bind='127.0.0.1:0',
            rpc_workers=0,
            concurrent_tasks=cls.concurrent_tasks,
            max_requests=cls.max_requests,
            message_serializer=cls.message_serializer,
            rpc_keep_alive=cls.rpc_timeout
        )
        pq = api.PulsarQueue(**params)
        await pq.start()
        cls.tq_app = pq.apps()[0]
        cls.rpc = pq.apps()[1]
        # make sure the time out is high enough (bigger than test-timeout)
        cls.proxy = rpc.JsonProxy('http://%s:%s' % cls.rpc.cfg.addresses[0],
                                  timeout=cls.rpc_timeout)
        # Now flush the task queue
        cls.api = cls.tq_app.backend
        await cls.api.tasks.flush_queues(*cls.queues())

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
        tasks = self.api.tasks
        self.assertTrue(isinstance(tasks.registry, dict))
        regular = tasks.registry.regular()
        periodic = tasks.registry.periodic()
        self.assertTrue(regular)
        self.assertTrue(periodic)

    def test_consumer(self):
        self.assertTrue(str(self.api).startswith('consumer <'))
        self.assertEqual(self.api.cfg.default_task_queue, '%s1' % self.name())

    def test_job_list(self):
        jobs = self.api.tasks.job_list()
        self.assertTrue(jobs)
        self.assertTrue(isinstance(jobs, list))
        d = dict(jobs)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'], 'regular')

    async def test_simple_task(self):
        tasks = self.api.tasks
        task = await tasks.queue('addition', a=40, b=50)
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, 90)
        self.assertTrue(str(task).startswith('task.addition<'))
        self.assertTrue(task.done())

    async def test_info(self):
        tasks = self.api.tasks
        task = await tasks.queue('workerinfo')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertIsInstance(task.result, dict)
        self.assertEqual(len(task.result['tasks']['queues']), 3)

    async def test_async_job(self):
        tasks = self.api.tasks
        result = tasks.queue('asynchronous', lag=2)
        self.assertIsInstance(result, asyncio.Future)
        task = await result
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertTrue(task.result >= 2)

    async def test_failure(self):
        tasks = self.api.tasks
        task = await tasks.queue('testperiodicerror', msg='testing')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'FAILURE')
        self.assertEqual(task.result, 'testing')
        self.assertTrue(task.stacktrace)

    async def test_execute_addition(self):
        tasks = self.api.tasks
        future = tasks.execute('addition', a=3, b=-4)
        self.assertIsInstance(future, api.MessageFuture)
        self.assertTrue(future.task_id)
        task = await future
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, -1)
        self.assertFalse(task.worker)
        self.assertFalse(task.queue)

    async def test_green_executor(self):
        tasks = self.api.tasks
        task = await tasks.queue('greenexecutor')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertIsInstance(task.result, dict)
        self.assertEqual(len(task.result['tasks']['queues']), 3)

    async def test_local_queue(self):
        tasks = self.api.tasks
        task = await tasks.queue_local('testlocalqueue')
        self.assertIsInstance(task, api.Task)
        self.assertIsInstance(task.result, list)
        self.assertEqual(len(task.result), 3)
        self.assertEqual(task.result[0], self.api.node_name)

    async def test_no_callback(self):
        tasks = self.api.tasks
        task = await tasks.queue('asynchronous', callback=False)
        self.assertTrue(task.id)
        self.assertEqual(task.status_string, 'QUEUED')
        self.assertTrue('ID=%s' % task.id in repr(task.done_callback))
        task = await task.done_callback
        self.assertEqual(task.status_string, 'SUCCESS')

    async def test_cpubound_task(self):
        tasks = self.api.tasks
        task = await tasks.queue('cpubound')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, ['OK', 2])

    async def __test_error_cpubound_task(self):
        tasks = self.api.tasks
        task = await tasks.queue('cpubound', error=True)
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'FAILURE')
        self.assertTrue(task.stacktrace)

    async def test_is_in_greenlet(self):
        tasks = self.api.tasks
        task = await tasks.queue('cpuboundwithasync')
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, True)

    async def test_supports_asyncio(self):
        tasks = self.api.tasks
        task = await tasks.queue('cpuboundwithasync', asyncio=True)
        self.assertIsInstance(task, api.Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, False)

    async def test_big_log(self):
        # If this test fails, it is because the test runner will timeout on
        # this future, this is because the pipe fills up and blocks the
        # cpu bound task
        tasks = self.api.tasks
        await tasks.queue('cpuboundbiglog')

    async def test_execute_python_code(self):
        tasks = self.api.tasks
        task = await tasks.execute('execute.python',
                                   code='print("Hello World!")')
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, 'Hello World!\n')

    async def test_execute_python_script(self):
        script = os.path.join(PATH, 'example', 'executable.py')
        tasks = self.api.tasks
        task = await tasks.execute('execute.python.script', script=script)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, sys.executable)

    async def test_queue_size(self):
        code = "import time;time.sleep(1)"
        tasks = self.api.tasks
        task = await asyncio.gather(
            tasks.queue('execute.python', code=code, callback=False),
            tasks.queue('execute.python', code=code, callback=False),
            tasks.queue('execute.python', code=code, callback=False),
            tasks.queue('execute.python', code=code, callback=False)
        )
        self.assertEqual(task[0].status_string, 'QUEUED')
        size = await self.api.broker.size(task[0].queue)
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
        tasks = self.api.tasks
        results = await asyncio.gather(
            tasks.queue('notoverlap'),
            tasks.queue('notoverlap'),
            tasks.queue('notoverlap')
        )
        results = sorted(results, key=lambda task: task.result['start'])
        self.assertTrue(results[0].result['end'] < results[1].result['start'])
        self.assertTrue(results[1].result['end'] < results[2].result['start'])

    async def test_queue_from_task(self):
        tasks = self.api.tasks
        task = await tasks.queue('queue.from.task')
        self.assertEqual(task.status_string, 'SUCCESS')
        other_task = task.result
        self.assertEqual(other_task['from_task'], task.id)

    async def test_scrape(self):
        tasks = self.api.tasks
        task = await tasks.queue('scrape', url='https://www.bbc.co.uk/')
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertTrue(task.result)

    async def test_delay(self):
        tasks = self.api.tasks
        task = await tasks.queue('scrape',
                                 delay=5,
                                 url='https://www.bbc.co.uk/')
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.delay, 5)
        self.assertTrue(task.time_started - task.time_queued > 5)
        self.assertTrue(task.result)

    async def test_thread_io(self):
        output = os.path.join(PATH, 'test.txt')
        tasks = self.api.tasks
        task = await tasks.queue(
            'extract.docx',
            input=os.path.join(PATH, 'example', 'test.docx'),
            output=output
        )
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertTrue(os.path.isfile(output))
        os.remove(output)
        self.assertNotEqual(task.result['thread'], threading.get_ident())
        self.assertEqual(task.result['text'], 306)

    async def test_bad_task(self):
        tasks = self.api.tasks
        task = await tasks.queue('asynchronous', sleep=2)
        self.assertEqual(task.status_string, 'FAILURE')

    async def test_retry(self):
        meta = {'max_retries': 3, 'retry_delay': 1}
        done = create_future()

        class CheckRetry:
            count = 1
            task_id = None

            def __call__(self, _, event, task):
                if task.name == 'subtraction':
                    if task.meta.get('from_task') == self.task_id:
                        self.count += 1
                        if task.retry == 3:
                            done.set_result(task)
                        else:
                            self.task_id = task.id

        check_retry = CheckRetry()
        await self.api.on_events('task', 'done', check_retry)
        try:
            task = await self.api.tasks.queue('subtraction', a=1, b='foo',
                                              delay=1,
                                              callback=False,
                                              meta_params=meta)
            self.assertEqual(task.status_string, 'QUEUED')
            check_retry.task_id = task.id
            task = await done
            self.assertEqual(check_retry.count, 3)
            self.assertEqual(task.status_string, 'FAILURE')
        finally:
            await self.api.remove_event_callback('task', 'done', check_retry)

    async def test_max_concurrency(self):
        tasks = [self.api.tasks.queue('maxconcurrency', lag=2)
                 for _ in range(5)]
        tasks = await asyncio.gather(*tasks)
        self.assertEqual(len(tasks), 5)
        revoked = success = 0
        for task in tasks:
            if task.status_string == 'REVOKED':
                revoked += 1
            elif task.status_string == 'SUCCESS':
                success += 1
        self.assertEqual(revoked, 2)
        self.assertEqual(success, 3)

    async def test_task_timeout(self):
        future = self.api.tasks.queue('asynchronous', lag=10, timeout=3)
        task = await future
        self.assertEqual(task.status_string, 'REVOKED')
        self.assertGreaterEqual(task.time_ended-task.time_queued,
                                task.timeout)
        self.assertTrue(task.expiry)

    # RPC
    async def test_rpc_job_list(self):
        data = await self.proxy.tasks.job_list()
        self.assertIsInstance(data, list)

    async def test_rpc_queue_task(self):
        task = await self.proxy.tasks.queue('cpubound')
        self.assertEqual(task['status'], 1)
