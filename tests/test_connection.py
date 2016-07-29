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
        if self.end:
            end, self.end = self.end, None
            end.set_result((args, kwargs))


class TestConnectionDrop(unittest.TestCase):
    app = None

    async def setUp(self):
        self.app = api.TaskApp(
            name='connection_%s' % random_string(),
            config='tests.config',
            workers=0
        )
        await self.app.start()
        self.backend = self.app._backend

    async def tearDown(self):
        if self.app:
            await send('arbiter', 'kill_actor', self.app.name)

    async def test_fail_get_task(self):
        publish, tester = self._patch(self.backend.broker, 'get_task')
        args, kw = await tester.end
        self.assertEqual(len(args), 3)
        self.assertEqual(args[1], self.backend.broker)

    async def test_fail_publish(self):
        get_task, tester = self._patch(self.backend.pubsub._pubsub, 'publish')
        task = self.backend.queue_task('addition', a=1, b=2)
        args, kw = await tester.end
        self.assertEqual(len(args), 3)
        self.assertEqual(args[1], self.backend.pubsub)
        task.cancel()

    def _log_error(self, coro, *args, **kwargs):
        coro.switch((args, kwargs))

    def _connection_error(self, *args, **kwargs):
        raise ConnectionRefusedError

    def _patch(self, obj, method):
        original = getattr(obj, method)
        setattr(obj, method, self._connection_error)
        tester = Tester()
        self.backend.logger.critical = tester
        return original, tester
