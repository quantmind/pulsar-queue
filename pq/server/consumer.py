import asyncio

from .producer import Producer


class Consumer(Producer):
    """The consumer is used by the server side application
    """
    def __repr__(self):
        return 'consumer <%s>' % self.broker

    @property
    def is_consumer(self):
        return True

    def tick(self):
        for consumer in self.consumers:
            consumer.tick()

    async def start(self, worker, consume=True):
        await self.pubsub.start()
        if consume:
            for consumer in self.consumers:
                consumer.start(worker)
        return self

    async def close(self, msg=None):
        if not self.closing():
            closing = []
            for consumer in self.consumers:
                closing.append(consumer.close())
            await asyncio.gather(*closing)
            self.manager.close()
