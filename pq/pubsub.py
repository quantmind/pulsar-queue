import asyncio

from .task import Task


class PubSub:
    '''Class implementing publish/subscribe for task producers
    '''
    def __init__(self, producer):
        self.cfg = producer.cfg
        self.logger = producer.logger
        self._callbacks = {}
        self._pubsub = producer.store.pubsub()
        self._client = producer.store.client()
        self._pubsub.add_client(self)
        asyncio.async(self._pubsub.psubscribe(self._channel('*')))

    @property
    def _loop(self):
        return self._pubsub._loop

    def queue(self, task):
        # Queue the task in the event loop and return a Future
        # called back once the task is done
        callback = asyncio.Future(loop=self._loop)
        self._callbacks[task.id] = callback
        asyncio.async(self._queue_task(task), loop=self._loop)
        return callback

    def get_task(self, *queues):
        '''Asynchronously retrieve a :class:`Task` from queues

        :return: a :class:`.Task` or ``None``.
        '''
        assert queues
        args = list(queues)
        args.append(self.cfg.task_pool_timeout)
        qt = yield from self._client.execute('brpop', *args)
        if qt:
            _, stask = qt
            return self.load(stask)

    def publish(self, event, task):
        '''Publish a task to the ``<prefix>_task_<event>`` channel

        :return: a coroutine
        '''
        channel = self._channel(event)
        stask = self.serialise(task)
        return self._pubsub.publish(channel, stask)

    def serialise(self, task):
        method = self.cfg.params.get('TASK_SERIALISATION')
        return task.serialise(method)

    def load(self, stask):
        method = self.cfg.params.get('TASK_SERIALISATION')
        return Task.load(stask, method)

    # INTERNALS
    def __call__(self, channel, message):
        # PubSub callback
        name = channel[len(self._channel()):]
        if name == 'done':
            task = Task.load(message)
            done = self._callbacks.pop(task.id, None)
            if done:
                done.set_result(task)

    def _queue_task(self, task):
        '''Asynchronously queue a task
        '''
        stask = self.serialise(task)
        yield from self.publish('queued', task)
        yield from self._client.lpush(task.queue, stask)
        self.logger.debug('queued %s in "%s"', task.lazy_info(), task.queue)

    def _channel(self, event=''):
        return '%s_task_%s' % (self.cfg.task_broadcast_prefix, event)
