"""Tests the api"""
import unittest

from pq import api


class TestTasks(unittest.TestCase):

    def test_decorator(self):
        Job = api.job('bla foo', v0=6)(lambda self, value=0: self.v0 + value)
        job = Job()
        self.assertIsInstance(job, api.Job)
        self.assertEqual(job(value=4), 10)
        self.assertEqual(str(job), 'bla.foo')
        self.assertFalse(job.task)
