from abc import ABC, abstractmethod
from asyncio import Future, ensure_future
from collections import OrderedDict

from pulsar.api import chain_future, ImproperlyConfigured
from pulsar.apps.http import HttpClient
from pulsar.apps.greenio import GreenPool
from pulsar.apps.data.channels import Connector
from pulsar.utils.importer import module_attribute

from .utils.serializers import serializers


class MessageFuture(Future):

    def __init__(self, message_id, backend, *, loop=None):
        super().__init__(loop=loop)
        self.message_id = message_id
        self.backend = backend

    def wait(self):     # pragma    nocover
        assert not self._loop.is_running(), 'cannot wait if loop is running'
        return self._loop.run_until_complete(_wait(self))

    def _repr_info(self):
        info = super()._repr_info()
        info.append('ID=%s' % self.message_id)
        return info


async def _wait(task_future):
    await task_future.backend.channels.connect()
    result = await task_future
    return result


class BaseComponent:

    def __init__(self, backend):
        self.backend = backend
        self.logger = self.backend.logger

    @property
    def cfg(self):
        return self.backend.cfg

    @property
    def _loop(self):
        return self.backend._loop

    def encode(self, message, serializer=None):
        """Encode a message"""
        serializer = serializer or self.cfg.message_serializer
        return serializers[serializer].encode(message)

    def decode(self, data, serializer=None):
        """Decode a message"""
        serializer = serializer or self.cfg.message_serializer
        return serializers[serializer].decode(data)


class Manager(BaseComponent):

    def green_pool(self):
        return GreenPool(loop=self._loop)

    def http(self):
        return HttpClient(loop=self._loop)

    def queues(self):
        """List of queue names for Message consumers
        """
        queues = [self.backend.node_name]
        queues.extend(self.cfg.task_queues)
        return queues

    async def store_message(self, message):
        """Dummy function to store a message into a persistent database
        """
        pass

    def start(self):
        """Optional start method, called by the backend when it starts
        """
        pass

    def close(self):
        pass


class MQ(BaseComponent, Connector, ABC):
    """Interface class for a distributed message queue
    """
    def __init__(self, backend, store, namespace=None):
        super().__init__(backend)
        Connector.__init__(self, store, namespace=namespace)
        self.store = store
        self.queued_messages = {}

    def __repr__(self):
        return 'message-broker - %s' % self.store

    def queue(self, message, callback=True):
        '''Queue the ``message``.

        If callback is True (default) returns a Future
        called back once the message is delivered,
        otherwise return a future called back once the messaged is queued
        '''
        future_done = MessageFuture(message.id, self.backend, loop=self._loop)
        if message.queue:
            self.queued_messages[message.id] = future_done
        else:   # the task is not queued instead it is executed immediately
            coro = self.backend.execute(message)
            return chain_future(coro, next=future_done)
        # queue the message
        coro = self._queue_message(message, future_done)
        if callback:
            ensure_future(coro, loop=self._loop)
            return future_done
        else:
            future = MessageFuture(message.id, self.backend, loop=self._loop)
            return chain_future(coro, next=future)

    @abstractmethod
    async def size(self, *queues):  # pragma    nocover
        '''Asynchronously retrieve the size of queues

        :return: the list of sizes
        '''
        pass

    @abstractmethod
    async def get_message(self, *queues):  # pragma    nocover
        '''Asynchronously retrieve a :class:`.Task` from queues

        :return: a :class:`.Task` or ``None``.
        '''
        pass

    @abstractmethod
    async def flush_queues(self, *queues):  # pragma    nocover
        '''Clear a list of task queues
        '''
        pass

    @abstractmethod
    async def queue_message(self, queue, message):  # pragma    nocover
        """Add a message to the ``queue``
        """
        pass

    @abstractmethod
    async def incr(self, name):
        """Increase the counter for name
        """
        pass

    @abstractmethod
    async def decr(self, name):
        """Decrease the counter for name
        """
        pass

    # INTERNALS
    async def _queue_message(self, message, future):
        '''Asynchronously queue a task
        '''
        await self.backend.publish('queued', message)
        try:
            await self.queue_message(message.queue, self.encode(message))
        except ConnectionRefusedError:
            self.logger.critical('Could not queue task - connection error')
        else:
            self.logger.debug('%s in "%s"', message.lazy_info(), message.queue)
        message.done_callback = future
        return message


def register_broker(name, factory=None):
    if factory is None:
        dotted_path = brokers.get(name)
        if not dotted_path:
            raise ImproperlyConfigured('No such message broker: %s' % name)
        factory = module_attribute(dotted_path, safe=True)
        if not factory:
            raise ImproperlyConfigured(
                '"%s" store not available' % dotted_path)
    else:
        brokers[name] = factory
    return factory


brokers = OrderedDict()
