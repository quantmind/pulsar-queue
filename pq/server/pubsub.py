from ..utils.serializers import Message
from ..mq import Component


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

    async def publish(self, event, message):
        '''Publish a task to the ``<prefix>_task_<event>`` channel

        :return: a coroutine
        '''
        if isinstance(message, Message):
            event = '%s_%s' % (message.type, event)
            await self.backend.manager.store_message(message)
        channel = self._channel(event)
        await self._pubsub.publish(channel, self.encode(message))

    # INTERNALS
    def __call__(self, channel, message):
        # PubSub callback
        event = channel[len(self._channel()):]
        message = self.decode(message)
        if event == 'task_done':
            done = self._callbacks.pop(message.id, None)
            if done:
                done.set_result(message)
        for callback in self._event_callbacks:
            try:
                callback(event, message)
            except Exception:
                self.logger.exception('During %s callbacks', event)

    def _channel(self, event=''):
        prefix = self.cfg.name
        return '%s_%s' % (prefix, event) if prefix else event
