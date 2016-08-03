from abc import ABC, abstractmethod
from asyncio import Future, ensure_future

from pulsar import chain_future
from pulsar.apps.http import HttpClient
from pulsar.apps.greenio import GreenPool

from .utils.serializers import serializers


class TaskFuture(Future):

    def __init__(self, task_id, backend, *, loop=None):
        super().__init__(loop=loop)
        self.task_id = task_id
        self.backend = backend

    def wait(self):     # pragma    nocover
        assert not self._loop.is_running(), 'cannot wait if loop is running'
        return self._loop.run_until_complete(_wait(self))

    def _repr_info(self):
        info = super()._repr_info()
        info.append('ID=%s' % self.task_id)
        return info


async def _wait(task_future):
    await task_future.backend.pubsub.start()
    result = await task_future
    return result


class BaseComponent:

    def __init__(self, backend):
        self.backend = backend

    @property
    def cfg(self):
        return self.backend.cfg

    @property
    def logger(self):
        return self.backend.logger

    def encode(self, message, serializer=None):
        """Encode a message"""
        serializer = serializer or self.cfg.message_serializer
        return serializers[serializer].encode(message)

    def decode(self, data, serializer=None):
        """Decode a message"""
        serializer = serializer or self.cfg.message_serializer
        return serializers[serializer].decode(data)


class TaskManager(BaseComponent):

    @property
    def _loop(self):
        return self.backend._loop

    def green_pool(self):
        return GreenPool(loop=self._loop)

    def http(self):
        return HttpClient(loop=self._loop)

    def queues(self):
        """List of queue names for Task consumers
        """
        queues = [self.backend.node_name]
        queues.extend(self.cfg.task_queues)
        return queues

    async def store_message(self, message):
        """Dummy function to store a message into a persistent database
        """
        pass

    def close(self):
        pass


class Component(BaseComponent):
    component_type = None

    def __init__(self, backend, store):
        super().__init__(backend)
        self.connection_error = False
        self.store = store

    def __repr__(self):
        return '%s - %s' % (self.component_type, self.store.dns)

    __str__ = __repr__

    @property
    def _loop(self):
        return self.store._loop

    def connection_ok(self):
        if self.connection_error:
            self.logger.warning(
                'connection with %s established - all good',
                self
            )
            self.connection_error = False
        else:
            return True


class MQ(Component, ABC):
    """Interface class for a distributed message queue
    """
    component_type = 'message-broker'

    @property
    def pubsub(self):
        return self.backend.pubsub

    @property
    def callbacks(self):
        return self.pubsub.callbacks

    def queue(self, task, callback=True):
        '''Queue the ``task``.

        If callback is True (default) returns a Future
        called back once the task is done, otherwise return a future
        called back once the task is queued
        '''
        future_done = TaskFuture(task.id, self.backend, loop=self._loop)
        if task.queue:
            self.callbacks[task.id] = future_done
        else:   # the task is not queued instead it is executed immediately
            coro = self.backend._execute_task(task)
            return chain_future(coro, next=future_done)
        coro = self._queue_task(task, future_done)
        if callback:
            ensure_future(coro, loop=self._loop)
            return future_done
        else:
            future = TaskFuture(task.id, self.backend, loop=self._loop)
            return chain_future(coro, next=future)

    @abstractmethod
    async def size(self, *queues):  # pragma    nocover
        '''Asynchronously retrieve the size of queues

        :return: the list of sizes
        '''
        pass

    @abstractmethod
    async def get_task(self, *queues):  # pragma    nocover
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

    # INTERNALS
    async def _queue_task(self, task, future):
        '''Asynchronously queue a task
        '''
        await self.pubsub.publish('queued', task)
        try:
            await self.queue_message(task.queue, self.encode(task))
        except ConnectionRefusedError:
            self.logger.critical('Could not queue task - connection error')
        else:
            self.logger.debug('%s in "%s"', task.lazy_info(), task.queue)
        task.done_callback = future
        return task
