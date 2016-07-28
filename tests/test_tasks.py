"""Tests the api"""
import unittest
from unittest import mock

from pq import api

from tests import simple_task


class TestTasks(unittest.TestCase):

    def app(self, task_paths=None, **kwargs):
        task_paths = task_paths or ['tests.example.sampletasks.*']
        app = api.TaskApp(task_paths=task_paths, **kwargs)
        app.backend.queue_task = mock.MagicMock()
        return app

    def test_decorator(self):
        job_cls = api.job('bla foo', v0=6)(simple_task)
        job = job_cls()
        self.assertIsInstance(job, api.Job)
        self.assertEqual(job(value=4), 10)
        self.assertEqual(str(job), 'bla.foo')
        self.assertFalse(job.task)
