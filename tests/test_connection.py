"""Tests connection errors"""
import unittest
from asyncio import Future

from pulsar import send
from pulsar.utils.string import random_string

from pq import api


class Tester:

    def __init__(self):
        self.end = Future()

    def __call__(self, *args, **kwargs):
        if not self.end.done():
            self.end.set_result((args, kwargs))


class TestConnectionDrop(unittest.TestCase):
    app = None

    async def setUp(self):
        self.app = api.QueueApp(
            name='connection_%s' % random_string(),
            config='tests.config',
            workers=0
        )
        await self.app.start()
        self.backend = self.app.backend

    async def tearDown(self):
        if self.app:
            await send('arbiter', 'kill_actor', self.app.name)

    async def test_fail_get_message(self):
        original, _, _ = self._patch(
            self.backend.broker, 'get_message')
        critical = Tester()
        self.backend.tasks.logger.critical = critical
        args, kw = await critical.end
        self.assertEqual(len(args), 3)
        self.assertEqual(args[1], self.backend.broker)
        self.assertEqual(args[2], 2)
        critical.end = Future()
        args, kw = await critical.end
        self.assertEqual(args[1], self.backend.broker)
        self.assertEqual(args[2], 2.25)

    async def test_fail_publish(self):
        original, warning, critical = self._patch(
            self.backend.channels.pubsub, 'publish')
        task = self.backend.tasks.queue('addition', a=1, b=2)
        args, kw = await critical.end
        self.assertEqual(len(args), 3)
        self.assertEqual(args[1], self.backend.channels)
        task.cancel()

    async def test_fail_subscribe(self):
        original, warning, critical = self._patch(
            self.backend.channels.pubsub, 'subscribe')
        await self.backend.on_events('tasks', 'started', lambda *args: args)
        args, kw = await critical.end
        self.assertEqual(len(args), 3)
        self.assertEqual(args[1], self.backend.channels)
        self.assertEqual(args[2], 2)
        critical.end = Future()
        args, kw = await critical.end
        self.assertEqual(len(args), 3)
        self.assertEqual(args[1], self.backend.channels)
        self.assertEqual(args[2], 2.25)
        self.backend.channels.pubsub.subscribe = original
        args, kw = await warning.end
        self.assertEqual(len(args), 3)
        self.assertEqual(args[1], self.backend.channels)
        self.assertEqual(args[2].name, 'consumer')

    def _log_error(self, coro, *args, **kwargs):
        coro.switch((args, kwargs))

    def _connection_error(self, *args, **kwargs):
        raise ConnectionRefusedError

    def _patch(self, obj, method):
        original = getattr(obj, method)
        setattr(obj, method, self._connection_error)
        critical = Tester()
        warning = Tester()
        self.backend.logger.critical = critical
        self.backend.logger.warning = warning
        return original, warning, critical
