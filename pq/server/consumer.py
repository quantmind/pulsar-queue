import time
import asyncio

from .producer import Producer


HEARTBEAT = 2
CONSUMER_EVENT = 'consumer_status'


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
            info['time'] = time.time()
            if self.cfg.debug:
                self.logger.debug('publishing worker %s info', worker)
            await self.pubsub.publish(CONSUMER_EVENT, info)
        finally:
            worker._loop.call_later(HEARTBEAT, self.__tick, worker)

    async def start(self, worker, consume=True):
        await self.pubsub.start()
        if consume:
            for consumer in self.consumers:
                consumer.start(worker)
            await self.worker_tick(worker)
        return self

    async def close(self, msg=None):
        if not self.closing():
            closing = []
            for consumer in self.consumers:
                closing.append(consumer.close())
            await asyncio.gather(*closing)
            self.manager.close()

    def __tick(self, worker):
        asyncio.ensure_future(self.worker_tick(worker), loop=worker._loop)
