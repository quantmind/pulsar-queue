"""Tests task scheduling"""
import time
import asyncio
import unittest
from functools import partial

from tests import app


class TestScheduler(app.TaskQueueBase, unittest.TestCase):
    schedule_periodic = True

    def test_scheduler(self):
        scheduler = self.tq_app.backend
        self.assertEqual(scheduler.cfg.default_task_queue, '%s1' % self.name())
        self.assertTrue(scheduler.tasks.next_run)

    def test_next_scheduled(self):
        scheduler = self.tq_app.backend
        entry, t = scheduler.tasks.next_scheduled()
        self.assertEqual(entry, 'testperiodic')

    def test_next_scheduled_entries(self):
        scheduler = self.tq_app.backend
        entry, t = scheduler.tasks.next_scheduled(['anchoredeveryhour'])
        self.assertEqual(entry, 'anchoredeveryhour')
        self.assertTrue(t > 0)

    async def test_periodic(self):
        scheduler = self.tq_app.backend
        future = asyncio.Future()
        cbk = scheduler.on_events(partial(self._test_periodic, future))
        try:
            result = await future
            self.assertTrue(result < time.time())
        finally:
            cbs = scheduler.remove_event_callback(cbk)
            self.assertEqual(len(cbs), 0)

    async def test_rpc_next_scheduled_tasks(self):
        next = await self.proxy.tasks.next_scheduled_tasks()
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
        except Exception as exc:
            future.set_exception(exc)
        else:
            future.set_result(task.result)
