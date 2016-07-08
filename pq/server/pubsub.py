from ..mq import Component, Task


class PubSub(Component):
    '''Class implementing publish/subscribe for task producers
    '''
    def __init__(self, backend, store):
        super().__init__(backend, store)
        self._callbacks = {}
        self._event_callbacks = []
        self._pubsub = store.pubsub()
        self._pubsub.add_client(self)

    @property
    def callbacks(self):
        """Dictionary of callbacks for tasks"""
        return self._callbacks

    async def start(self):
        """Subscribe to all channels

        This a coroutine and must be waited
        """
        await self._pubsub.psubscribe(self._channel('*'))
        self.logger.info('%s ready and listening to events', self._pubsub)

    def on_events(self, callback):
        self._event_callbacks.append(callback)
        return callback

    def remove_event_callback(self, callback):
        if callback in self._event_callbacks:
            self._event_callbacks.remove(callback)
        return self._event_callbacks

    def lock(self, name, **kwargs):
        """Global distributed lock
        """
        return self.store.client().lock(name, **kwargs)

    async def publish(self, event, task):
        '''Publish a task to the ``<prefix>_task_<event>`` channel

        :return: a coroutine
        '''
        channel = self._channel(event)
        await self.backend.store_task(task)
        await self._pubsub.publish(channel, self.serialise(task))

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

    def _channel(self, event=''):
        event = 'task_%s' % event
        prefix = self.cfg.task_queue_prefix
        return '%s_%s' % (prefix, event) if prefix else event


async def store_task(task):
    """Dummy function to store a task into a persistent database
    """
    return
