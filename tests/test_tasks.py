"""Tests the api"""
import unittest

from pq import api

from tests import simple_task


class TestTasks(unittest.TestCase):
    def app(self, task_paths=None, **kwargs):
        task_paths = task_paths or ['tests.example.sampletasks.*']
        return api.TaskApp(task_paths=task_paths, **kwargs)

    def test_decorator(self):
        job_cls = api.job('bla foo', v0=6)(simple_task)
        job = job_cls()
        self.assertIsInstance(job, api.Job)
        self.assertEqual(job(value=4), 10)
        self.assertEqual(str(job), 'bla.foo')
        self.assertFalse(job.task)

    def test_periodic_entries(self):
        tq = self.app(schedule_periodic=True)
        self.assertTrue(tq.cfg.schedule_periodic)
        backend = tq.backend
        self.assertIsInstance(backend, api.TaskScheduler)
        entries = backend.entries
        self.assertTrue(entries)
        self.assertEqual(entries, backend.entries)
