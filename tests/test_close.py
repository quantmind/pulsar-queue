"""Tests closing the worker after 10 requests"""
import unittest
import asyncio
from random import random

from tests import app


class TestMsgPackQueue(app.TaskQueueBase, unittest.TestCase):
    max_requests = 10
    concurrent_tasks = 20

    async def test_max_requests(self):
        tasks = [self.tq.queue_task('asynchronous', lag=random())
                 for _ in range(18)]
        tasks = await asyncio.gather(*tasks)
        self.assertEqual(len(tasks), 18)
        workers = set()
        for task in tasks:
            self.assertEqual(task.status_string, 'SUCCESS')
            workers.add(task.worker)

        self.assertEqual(len(workers), 2)

        # FAILURES
        tasks = [self.tq.queue_task('asynchronous', sleep=1)
                 for _ in range(6)]

        tasks = await asyncio.gather(*tasks)
        self.assertEqual(len(tasks), 6)
        workers = set()
        for task in tasks:
            self.assertEqual(task.status_string, 'FAILURE')
            workers.add(task.worker)

        self.assertEqual(len(workers), 2)
