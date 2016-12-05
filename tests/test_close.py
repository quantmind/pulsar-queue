"""Tests closing the worker after 10 requests"""
import unittest
import asyncio
from random import random

from tests import app


def venti(cfg):
    return 20


class TestMsgPackQueue(app.TaskQueueBase, unittest.TestCase):
    max_requests = 10
    concurrent_tasks = 'pq.tests.test_close:venti'

    async def test_max_requests(self):
        api = self.api
        tasks = [api.tasks.queue('asynchronous', lag=random())
                 for _ in range(18)]
        tasks = await asyncio.gather(*tasks)
        self.assertEqual(len(tasks), 18)
        workers = set()
        for task in tasks:
            self.assertEqual(task.status_string, 'SUCCESS')
            workers.add(task.worker)

        self.assertEqual(len(workers), 2)

        # FAILURES
        tasks = [api.tasks.queue('asynchronous', sleep=1)
                 for _ in range(6)]

        tasks = await asyncio.gather(*tasks)
        self.assertEqual(len(tasks), 6)
        workers = set()
        for task in tasks:
            self.assertEqual(task.status_string, 'FAILURE')
            workers.add(task.worker)

        self.assertEqual(len(workers), 2)
