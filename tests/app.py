'''Tests the taskqueue local backend.'''
import unittest
import asyncio
from random import random

from pulsar import send, multi_async
from pulsar.apps.test import dont_run_with_thread
from pulsar.apps import rpc

from pq import PulsarQueue, Task, TaskFuture, nice_task_message, states


CODE_TEST = '''\
import time
def task_function(N = 10, lag = 0.1):
    time.sleep(lag)
    return N*N
'''


def dummy():
    # Just a dummy callable for testing coverage.
    # A callable is invoked when the taskqueue starts
    pass


class TaskQueueBase(object):
    concurrency = 'thread'
    # used for both keep-alive and timeout in JsonProxy
    # long enough to allow to wait for tasks
    rpc_timeout = 500
    tq = None
    rpc = None

    @classmethod
    def name(cls):
        return cls.__name__.lower()

    @classmethod
    def task_backend(cls):
        return None

    @classmethod
    def rpc_name(cls):
        return 'rpc_%s' % cls.name()

    @classmethod
    def setUpClass(cls):
        # The name of the task queue application
        name = cls.name()
        queues = ['%s1' % name, '%s2' % name]
        pq = PulsarQueue(cls.name(),
                         wsgi=True,
                         queue_callable=dummy,
                         task_queues=queues,
                         default_task_queue=queues[0],
                         rpc_bind='127.0.0.1:0',
                         concurrency=cls.concurrency,
                         rpc_concurrency=cls.concurrency,
                         rpc_keep_alive=cls.rpc_timeout,
                         task_paths=['tests.example.sampletasks.*'])
        cfgs = yield from pq.start()
        cls.tq = cfgs[0].app()
        cls.rpc = cfgs[1].app()
        # make sure the time out is high enough (bigger than test-timeout)
        cls.proxy = rpc.JsonProxy('http://%s:%s' % cls.rpc.cfg.addresses[0],
                                  timeout=cls.rpc_timeout)
        # Now flush the task queue
        backend = cls.tq.backend
        yield from backend.flush_queues(*queues)

    @classmethod
    def tearDownClass(cls):
        return multi_async((send('arbiter', 'kill_actor', a.name)
                           for a in (cls.tq, cls.rpc) if a is not None))


class TestTaskQueueOnThread(TaskQueueBase, unittest.TestCase):

    def test_registry(self):
        backend = self.tq.backend
        self.assertTrue(isinstance(backend.registry, dict))
        regular = backend.registry.regular()
        periodic = backend.registry.periodic()
        self.assertTrue(regular)
        self.assertTrue(periodic)

    def test_producer(self):
        backend = self.tq.backend
        self.assertTrue(str(backend).startswith('task producer <'))
        self.assertEqual(self.tq.cfg.default_task_queue, '%s1' % self.name())

    def test_job_list(self):
        jobs = self.tq.backend.job_list()
        self.assertTrue(jobs)
        self.assertTrue(isinstance(jobs, list))
        d = dict(jobs)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'], 'regular')

    def test_simple_task(self):
        task = yield from self.tq.queue_task('addition', a=40, b=50)
        self.assertIsInstance(task, Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, 90)
        self.assertTrue(str(task).startswith('task.addition<'))
        self.assertTrue(task.done())

    def test_simple_revoked(self):
        task = yield from self.tq.queue_task('addition', a=40, b=50,
                                             expiry=0)
        self.assertIsInstance(task, Task)
        self.assertEqual(task.status_string, 'REVOKED')
        self.assertFalse(task.result)

    def test_info(self):
        task = yield from self.tq.queue_task('workerinfo')
        self.assertIsInstance(task, Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertIsInstance(task.result, dict)
        self.assertEqual(len(task.result['queues']), 2)

    def test_async_job(self):
        result = self.tq.queue_task('asynchronous', lag=2)
        self.assertIsInstance(result, asyncio.Future)
        task = yield from result
        self.assertIsInstance(task, Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertTrue(task.result >= 2)

    def test_failure(self):
        task = yield from self.tq.queue_task('testperiodicerror',
                                             msg='testing')
        self.assertIsInstance(task, Task)
        self.assertEqual(task.status_string, 'FAILURE')
        self.assertEqual(task.result, 'testing')
        self.assertTrue(task.stacktrace)

    def test_execute_addition(self):
        future = self.tq.execute_task('addition', a=3, b=-4)
        self.assertIsInstance(future, TaskFuture)
        self.assertTrue(future.task_id)
        task = yield from future
        self.assertIsInstance(task, Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, -1)
        self.assertFalse(task.worker)
        self.assertFalse(task.queue)

    def test_green_executor(self):
        task = yield from self.tq.queue_task('greenexecutor')
        self.assertIsInstance(task, Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertIsInstance(task.result, dict)
        self.assertEqual(len(task.result['queues']), 2)


class d:
    #    RPC TESTS
    def test_check_next_run(self):
        app = self.tq
        backend = app.backend
        backend.tick()

    def test_rpc_ping(self):
        return self.async.assertEqual(self.proxy.ping(), 'pong')

    def test_rpc_job_list(self):
        jobs = yield self.proxy.job_list()
        self.assertTrue(jobs)
        self.assertTrue(isinstance(jobs, list))
        d = dict(jobs)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'], 'regular')

    def test_rpc_job_list_with_names(self):
        jobs = yield self.proxy.job_list(jobnames=['runpycode'])
        self.assertEqual(len(jobs), 1)
        jobs = yield self.proxy.job_list(jobnames=['xxxxxx'])
        self.assertEqual(len(jobs), 0)

    def test_rpc_next_scheduled_tasks(self):
        next = yield self.proxy.next_scheduled_tasks()
        self.assertTrue(next)
        self.assertEqual(len(next), 2)
        next = yield self.proxy.next_scheduled_tasks(jobnames=['testperiodic'])
        self.assertTrue(next)
        self.assertEqual(len(next), 2)
        self.assertEqual(next[0], 'testperiodic')
        self.assertTrue(next[1] >= 0)

    def test_ping_store(self):
        tq = self.tq
        self.assertTrue(tq.backend)
        backend = tq.backend
        store = backend.store
        client = store.client()
        self.async.assertEqual(client.ping(), True)

    def test_pickled_app(self):
        tq = self.tq
        self.assertEqual(tq.name, self.name())
        self.assertTrue(tq.backend)
        backend = tq.backend
        self.assertEqual(backend.poll_timeout, 2)
        self.assertEqual(backend.num_concurrent_tasks, 0)
        self.assertEqual(backend.backlog, self.concurrent_tasks)

    def test_meta(self):
        '''Tests meta attributes of taskqueue'''
        app = self.tq
        self.assertTrue(app)
        self.assertEqual(app.name, self.name())
        self.assertFalse(app.cfg.address)
        self.assertEqual(app.cfg.concurrent_tasks, self.concurrent_tasks)
        self.assertEqual(app.backend.backlog, self.concurrent_tasks)
        self.assertTrue(app.backend.registry)
        self.assertEqual(app.cfg.concurrency, self.concurrency)
        job = app.backend.registry['runpycode']
        self.assertEqual(job.type, 'regular')
        self.assertTrue(job.can_overlap)
        id, oid = app.backend.generate_task_ids(job, {})
        self.assertTrue(id)
        self.assertFalse(oid)
        id1, oid = app.backend.generate_task_ids(job, {})
        self.assertNotEqual(id, id1)
        self.assertFalse(oid)

    def test_rpc_meta(self):
        app = self.rpc
        cfg = app.cfg
        self.assertTrue(app)
        self.assertEqual(app.name, self.rpc_name())
        self.assertEqual(cfg.address, ('127.0.0.1', 0))
        self.assertNotEqual(cfg.addresses[0], cfg.address)
        self.assertEqual(cfg.concurrency, self.concurrency)
        wsgi_handler = cfg.callable.handler()
        self.assertEqual(len(wsgi_handler.middleware), 1)
        router = wsgi_handler.middleware[0]
        self.assertTrue(router.post)
        root = router.post
        tq = root.taskqueue
        self.assertEqual(tq, self.name())

    def test_queue_task_expiry(self):
        r = yield self.proxy.queue_task(jobname='addition', a=40, b=50,
                                        expiry=0)
        self.assertTrue(r)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], states.REVOKED)

    def test_run_new_simple_task_from_test(self):
        app = self.tq
        r = yield app.backend.queue_task('addition', a=1, b=2)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], states.SUCCESS)
        self.assertEqual(r['result'], 3)

    def test_not_overlap(self):
        sec = 2 + random()
        app = self.tq
        self.assertEqual(app.name, app.backend.name)
        self.assertTrue('notoverlap' in app.backend.registry)
        r1 = yield app.backend.queue_task('notoverlap', lag=sec)
        self.assertTrue(r1)
        r2 = yield app.backend.queue_task('notoverlap', lag=sec)
        self.assertFalse(r2)
        # We need to make sure the first task is completed
        r1 = yield app.backend.wait_for_task(r1)
        self.assertEqual(r1['status'], states.SUCCESS)
        self.assertTrue(r1['result'] > sec)

    def test_queue_task_error(self):
        yield self.async.assertRaises(rpc.InvalidParams,
                                      self.proxy.queue_task)
        yield self.async.assertRaises(rpc.InternalError,
                                      self.proxy.queue_task,
                                      jobname='xxxx', bla='foo')

    def test_queue_task_run_py_code(self):
        '''Run a new task from the *runpycode* task factory.'''
        r = yield self.proxy.queue_task(jobname='runpycode',
                                        code=CODE_TEST, N=3)
        self.assertTrue(r)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], states.SUCCESS)
        self.assertEqual(r['result'], 9)

    def test_queue_task_periodicerror(self):
        r = yield self.proxy.queue_task(jobname='testperiodicerror')
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], states.FAILURE)
        self.assertTrue('kaputt' in r['result'])

    def __test_delete_task(self):
        # ISSUE #56
        app = self.tq
        id = yield app.backend.queue_task(jobname='addition', a=1, b=4)
        r1 = yield app.backend.wait_for_task(id)
        self.assertEqual(r1.result, 5)
        deleted = yield app.backend.delete_tasks([r1.id, 'kjhbkjb'])
        self.assertEqual(len(deleted), 1)
        r1 = yield app.backend.get_task(r1.id)
        self.assertFalse(r1)

    def test_run_producerconsumer(self):
        '''A task which produce other tasks'''
        sample = 5
        r = yield self.proxy.queue_task(jobname='standarddeviation',
                                        sample=sample, size=100)
        self.assertTrue(r)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], states.SUCCESS)
        self.assertEqual(r['result'], 'produced %s new tasks' % sample)
        self.assertTrue(nice_task_message(r))
        # We check for the tasks created
        # TODO: not available
        # created = yield self.proxy.get_tasks(from_task=r['id'])
        # self.assertEqual(len(created), sample)
        # stasks = []
        # for task in created:
        #     stasks.append(self.proxy.wait_for_task(task['id']))
        # created = yield multi_async(stasks)
        # self.assertEqual(len(created), sample)
        # for task in created:
        #     self.assertEqual(task['status'], tasks.SUCCESS)

    def test_id_not_overlap(self):
        '''Check `generate_task_ids` when `can_overlap` attribute is set to
        False.'''
        app = self.tq
        job = app.backend.registry['notoverlap']
        self.assertEqual(job.type, 'regular')
        self.assertFalse(job.can_overlap)
        #
        id1, oid1 = app.backend.generate_task_ids(job, {})
        self.assertTrue(oid1)
        id2, oid2 = app.backend.generate_task_ids(job, {})
        self.assertTrue(oid2)
        self.assertNotEqual(id2, id1)
        self.assertEqual(oid2, oid1)
        #
        id3, oid3 = app.backend.generate_task_ids(job, {'p': 45})
        self.assertTrue(oid3)
        self.assertNotEqual(id3, id2)
        self.assertNotEqual(oid3, oid2)
        id4, oid4 = app.backend.generate_task_ids(job, {'p': 45})
        self.assertNotEqual(id4, id3)
        self.assertEqual(oid4, oid3)
        #
        id5, oid5 = app.backend.generate_task_ids(job, {'p': 45, 'c': 'bla'})
        self.assertTrue(oid5)
        id6, oid6 = app.backend.generate_task_ids(job, {'p': 45, 'c': 'bla'})
        id7, oid7 = app.backend.generate_task_ids(job, {'p': 45, 'd': 'bla'})
        id8, oid8 = app.backend.generate_task_ids(job, {'p': 45, 'c': 'blas'})
        #
        self.assertEqual(oid5, oid6)
        self.assertNotEqual(oid5, oid7)
        self.assertNotEqual(oid5, oid8)


@dont_run_with_thread
class TestTaskQueueOnProcess(TestTaskQueueOnThread):
    concurrency = 'process'

    def test_cpubound_task(self):
        task = yield from self.tq.queue_task('cpubound')
        self.assertIsInstance(task, Task)
        self.assertEqual(task.status_string, 'SUCCESS')
        self.assertEqual(task.result, ['OK', 2])

    def __test_error_cpubound_task(self):
        task = yield from self.tq.queue_task('cpubound', error=True)
        self.assertIsInstance(task, Task)
        self.assertEqual(task.status_string, 'FAILURE')
        self.assertTrue(task.stacktrace)
