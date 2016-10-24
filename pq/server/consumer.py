import time
import asyncio
from multiprocessing import cpu_count

from .producer import Producer, ConsumerMessage


HEARTBEAT = 2


class Consumer(Producer):
    """The consumer is used by the server side application
    """
    def __repr__(self):
        return 'consumer <%s>' % self.broker

    @property
    def is_consumer(self):
        return True

    def tick(self, monitor):
        for consumer in self.consumers:
            consumer.tick()

    async def worker_tick(self, worker):
        try:
            info = dict(self.info())
            info['worker'] = worker.aid
            info['node'] = self.node_name
            info['pubsub'] = self.channels.dns
            info['cores'] = cpu_count()
            info['message-broker'] = self.broker.store.dns
            info['time'] = time.time()
            if self.cfg.debug:
                self.logger.debug('publishing worker %s info', worker)
            await self.publish('status', ConsumerMessage(info))
        finally:
            worker._loop.call_later(HEARTBEAT, self.__tick, worker)

    async def start(self, worker, consume=True):
        await super().start()
        if consume:
            for consumer in self.consumers:
                consumer.start(worker)
            await self.worker_tick(worker)
        return self

    def __tick(self, worker):
        asyncio.ensure_future(self.worker_tick(worker), loop=worker._loop)
