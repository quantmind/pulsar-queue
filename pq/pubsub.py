import asyncio

from .task import Task


class PubSub:
    '''Class implementing publish/subscribe for task producers
    '''
    def __init__(self, producer):
        self.cfg = producer.cfg
        self.logger = producer.logger
        self._callbacks = {}
        self._event_callbacks = []
        self._pubsub = producer.store.pubsub()
        self._client = producer.store.client()
        self._pubsub.add_client(self)
        asyncio.async(self._pubsub.psubscribe(self._channel('*')))

    @property
    def _loop(self):
        return self._pubsub._loop

    def on_events(self, callback):
        self._event_callbacks.append(callback)

    def remove_event_callback(self, callback):
        if callback in self._event_callbacks:
            self._event_callbacks.remove(callback)

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

    def flush_queues(self, *queues):
        '''Clear a list of task queues
        '''
        pipe = self._client.pipeline()
        for queue in queues:
            pipe.execute('del', queue)
        return pipe.commit()

    # INTERNALS
    def __call__(self, channel, message):
        # PubSub callback
        event = channel[len(self._channel()):]
        task = Task.load(message)
        if event == 'done':
            done = self._callbacks.pop(task.id, None)
            if done:
                done.set_result(task)
        for callback in self._event_callbacks:
            try:
                callback(event, task)
            except Exception:
                self.logger.exception('During %s callbacks', task)

    def _queue_task(self, task):
        '''Asynchronously queue a task
        '''
        stask = self.serialise(task)
        yield from self.publish('queued', task)
        yield from self._client.lpush(task.queue, stask)
        self.logger.debug('%s in "%s"', task.lazy_info(), task.queue)

    def _channel(self, event=''):
        event = 'task_%s' % event
        prefix = self.cfg.task_queue_prefix
        return '%s_%s' % (prefix, event) if prefix else event
