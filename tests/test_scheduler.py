import time
import asyncio
import unittest
from functools import partial

from tests.test_app import TaskQueueBase


class TestScheduler(TaskQueueBase, unittest.TestCase):
    schedule_periodic = True

    def test_scheduler(self):
        scheduler = self.tq_app.scheduler()
        self.assertTrue(str(scheduler).startswith('task scheduler <'))
        self.assertEqual(scheduler.cfg.default_task_queue, '%s1' % self.name())
        self.assertTrue(scheduler.next_run)

    def test_next_scheduled(self):
        scheduler = self.tq_app.scheduler()
        entry, t = scheduler.next_scheduled()
        self.assertEqual(entry, 'testperiodic')

    def test_next_scheduled_entries(self):
        scheduler = self.tq_app.scheduler()
        entry, t = scheduler.next_scheduled(['anchoredeveryhour'])
        self.assertEqual(entry, 'anchoredeveryhour')
        self.assertTrue(t > 0)

    async def test_test_periodic(self):
        future = asyncio.Future()
        self.cbk = self.tq.pubsub.on_events(
            partial(self._test_periodic, future))
        result = await future
        self.assertTrue(result < time.time())

    async def test_rpc_next_scheduled_tasks(self):
        next = await self.proxy.next_scheduled_tasks()
        self.assertTrue(isinstance(next, list))
        self.assertEqual(len(next), 2)
        self.assertEqual(next[0], 'testperiodic')

    def _test_periodic(self, future, event, task):
        if not event.startswith('task_'):
            return
        try:
            self.assertEqual(task.name, 'testperiodic')
            if event != 'task_done':
                return
            cbs = self.tq.pubsub.remove_event_callback(self.cbk)
            self.assertEqual(len(cbs), 0)
        except Exception as exc:
            future.set_exception(exc)
        else:
            future.set_result(task.result)
