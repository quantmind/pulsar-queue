"""Tests the api"""
import unittest
from datetime import datetime
from unittest import mock

from pq import api
from pq.utils.time import format_time

from tests.app import simple_task


class TestTasks(unittest.TestCase):

    def app(self, task_paths=None, **kwargs):
        task_paths = task_paths or ['tests.example.sampletasks.*']
        app = api.TaskApp(task_paths=task_paths, **kwargs)
        app.backend.tasks.queue = mock.MagicMock()
        return app

    def test_decorator(self):
        job_cls = api.job('bla foo', v0=6)(simple_task)
        job = job_cls()
        self.assertIsInstance(job, api.Job)
        self.assertEqual(job(value=4), 10)
        self.assertEqual(str(job), 'bla.foo')
        self.assertFalse(job.task)

    def test_unknown_state(self):
        self.assertEqual(api.status_string(243134), 'UNKNOWN')
        self.assertEqual(api.status_string('jhbjhbj'), 'UNKNOWN')
        self.assertEqual(api.status_string(1), 'SUCCESS')

    def test_format_time(self):
        dt = datetime.now()
        st = format_time(dt)
        self.assertIsInstance(st, str)
        timestamp = dt.timestamp()
        st2 = format_time(timestamp)
        self.assertEqual(st, st2)
        self.assertEqual(format_time(None), '?')

    def test_close(self):
        t = api.TaskApp().api()
        self.assertEqual(t.closing(), False)
        t.close()
        self.assertEqual(t.closing(), True)
        self.assertEqual(t.tasks.closing(), True)
        warn = mock.MagicMock()
        t.logger.warning = warn
        self.assertFalse(t.tasks.queue('foo'))
        self.assertEqual(warn.call_count, 1)
        self.assertEqual(
            warn.call_args[0][0],
            'Cannot queue task, task backend closing'
        )

    def test_task_not_available(self):
        t = api.TaskApp().api()
        self.assertRaises(api.TaskNotAvailable,
                          t.tasks.queue, 'jsdbcjsdhbc')

    def test_queues(self):
        t = api.TaskApp().api()
        self.assertTrue(t.tasks.queues())
