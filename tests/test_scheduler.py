import time
import asyncio
import unittest
from functools import partial

from tests.test_app import TaskQueueBase


class TestScheduler(TaskQueueBase, unittest.TestCase):
    schedule_periodic = True

    def test_scheduler(self):
        backend = self.tq.backend
        self.assertTrue(str(backend).startswith('task scheduler <'))
        self.assertEqual(self.tq.cfg.default_task_queue, '%s1' % self.name())
        self.assertTrue(backend.next_run)

    def test_next_scheduled(self):
        backend = self.tq.backend
        entry, t = backend.next_scheduled()
        self.assertEqual(entry, 'testperiodic')

    def test_next_scheduled_entries(self):
        backend = self.tq.backend
        entry, t = backend.next_scheduled(['anchoredeveryhour'])
        self.assertEqual(entry, 'anchoredeveryhour')
        self.assertTrue(t > 0)

    async def test_test_periodic(self):
        backend = self.tq.backend
        future = asyncio.Future()
        self.cbk = backend.pubsub.on_events(
            partial(self._test_periodic, future))
        result = await future
        self.assertTrue(result < time.time())

    def _test_periodic(self, future, event, task):
        try:
            self.assertEqual(task.name, 'testperiodic')
            if event != 'done':
                return
            cbs = self.tq.backend.pubsub.remove_event_callback(self.cbk)
            self.assertEqual(len(cbs), 0)
        except Exception as exc:
            future.set_exception(exc)
        else:
            future.set_result(task.result)
