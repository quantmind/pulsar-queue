import unittest

import pq


class TestTasks(unittest.TestCase):

    def test_decorator(self):
        Job = pq.job('bla foo', v0=6)(lambda self, value=0: self.v0 + value)
        job = Job()
        self.assertIsInstance(job, pq.Job)
        self.assertEqual(job(value=4), 10)
        self.assertEqual(str(job), 'bla.foo')
        self.assertFalse(job.task)
