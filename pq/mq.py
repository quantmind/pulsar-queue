from abc import ABC, abstractmethod
from asyncio import Future, ensure_future

from pulsar import chain_future

from .tasks.task import Task


class TaskFuture(Future):

    def __init__(self, task_id, *, loop=None):
        super().__init__(loop=loop)
        self.task_id = task_id

    def wait(self):
        assert not self._loop.is_running(), 'cannot wait if loop is running'
        return self._loop.run_until_complete(self)

    def _repr_info(self):
        info = super()._repr_info()
        info.append('ID=%s' % self.task_id)
        return info


class Component:

    def __init__(self, backend, store):
        self.backend = backend
        self.store = store

    def __repr__(self):
        return self.store.dns

    __str__ = __repr__

    @property
    def cfg(self):
        return self.backend.cfg

    @property
    def logger(self):
        return self.backend.logger

    @property
    def _loop(self):
        return self.store._loop

    def serialise(self, task):
        method = self.cfg.params.get('TASK_SERIALISATION')
        return task.serialise(method)

    def load(self, stask):
        method = self.cfg.params.get('TASK_SERIALISATION')
        return Task.load(stask, method)


class MQ(Component, ABC):
    """Interface class for a distributed message queue
    """
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
        if callback:
            callback = TaskFuture(task.id, loop=self._loop)
            if task.queue:
                self.callbacks[task.id] = callback
            else:   # the task is not queued instead it is executed immediately
                coro = self.backend._execute_task(task)
                return chain_future(coro, next=callback)
        result = ensure_future(self._queue_task(task), loop=self._loop)
        return callback or result

    @abstractmethod
    async def get_task(self, *queues):
        '''Asynchronously retrieve a :class:`.Task` from queues

        :return: a :class:`.Task` or ``None``.
        '''

    @abstractmethod
    async def flush_queues(self, *queues):
        '''Clear a list of task queues
        '''

    @abstractmethod
    async def queue_message(self, queue, message):
        """Add a message to the ``queue``
        """
        pass

    # INTERNALS
    async def _queue_task(self, task):
        '''Asynchronously queue a task
        '''
        stask = self.serialise(task)
        await self.pubsub.publish('queued', task)
        await self.queue_message(task.queue, stask)
        self.logger.debug('%s in "%s"', task.lazy_info(), task.queue)
        return task
