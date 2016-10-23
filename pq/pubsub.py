from pulsar import as_coroutine
from pulsar.apps.data.channels import Channels

from .mq import Component
from .utils.serializers import MessageDict

RECONNECT_LAG = 2


def backoff(value):
    return min(value + 0.25, 16)


HEARTBEAT = 2


class ConsumerMessage(MessageDict):
    type = 'consumer'


class PubSub(Component):
    '''Class implementing publish/subscribe for task producers
    '''
    component_type = 'pubsub'

    def __init__(self, backend, store):
        super().__init__(backend, store)
        self.channels = Channels(
            store.pubsub(protocol=self),
            namespace=self.cfg.name,
            status_channel=ConsumerMessage.type,
            logger=self.logger
        )
        self.callbacks = {}

    @property
    def pubsub(self):
        return self.channels.pubsub

    def on_events(self, channel, event, callback):
        return self._loop.create_task(
            self.channels.register(channel, event, callback)
        )

    def remove_event_callback(self, channel, event, callback):
        return self._loop.create_task(
            self.channels.unregister(channel, event, callback)
        )

    async def start(self):
        """Subscribe to all channels

        This a coroutine and must be waited
        """
        for consumer in self.backend.consumers:
            await as_coroutine(consumer.register())
        await self.channels.connect()

    async def close(self):
        await self.channels.close()

    def lock(self, name, **kwargs):
        """Global distributed lock
        """
        return self.channels.lock(name, **kwargs)

    async def publish(self, event, message):
        """Publish an event to the message channel
        """
        await self.backend.manager.store_message(message)
        await self.channels.publish(message.type, event, message)
