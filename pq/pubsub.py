from .utils.serializers import Message
from .mq import Component


RECONNECT_LAG = 2


def backoff(value):
    return min(value + 0.25, 16)


class PubSub(Component):
    '''Class implementing publish/subscribe for task producers
    '''
    component_type = 'pubsub'

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

    async def start(self, next_time=None):
        """Subscribe to all channels

        This a coroutine and must be waited
        """
        self._pubsub.bind_event('connection_lost', self._connection_lost)
        pattern = self.prefixed('*')
        try:
            await self._pubsub.psubscribe(pattern)
            self.logger.warning(
                '%s ready and listening for events on %s - all good',
                self,
                pattern
            )
        except ConnectionRefusedError:
            next_time = backoff(next_time) if next_time else RECONNECT_LAG
            self.logger.critical(
                '%s cannot subscribe to %s - connection error - '
                'try again in %s seconds',
                self,
                pattern,
                next_time
            )
            self._loop.call_later(next_time, self.connect, next_time)

    async def close(self):
        self._pubsub.remove_callback('connection_lost', self._connection_lost)
        await self._pubsub.close()

    def connect(self, next_time=None):
        loop = self._loop
        if loop.is_running():
            loop.create_task(self.start(next_time))

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

        channel = self.prefixed(event)
        try:
            await self._pubsub.publish(channel, self.encode(message))
        except ConnectionRefusedError:
            self.connection_error = True
            self.logger.critical(
                '%s cannot publish on "%s" channel - connection error',
                self,
                channel
            )
        else:
            self.connection_ok()

    # INTERNALS
    def __call__(self, channel, message):
        # PubSub callback
        event = channel[len(self.prefixed()):]
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

    def _connection_lost(self, *args):
        self.connect()
