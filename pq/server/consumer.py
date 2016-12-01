import time
import asyncio
from multiprocessing import cpu_count

from pulsar.apps.data.channels import backoff

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

    async def worker_tick(self, worker, next=None):
        pnext, next = next, HEARTBEAT
        try:
            info = dict(self.info())
            info['worker'] = worker.aid
            info['node'] = self.node_name
            info['pubsub'] = str(self.channels)
            info['cores'] = cpu_count()
            info['message-broker'] = str(self.broker.store)
            info['time'] = time.time()
            if self.cfg.debug:
                self.logger.debug('publishing worker %s info', worker)
            await self.publish('status', ConsumerMessage(info))
        except ConnectionError:
            next = next if not pnext else backoff(pnext)
            self.logger.critical(
                'Cannot publish consumer status: connection error.'
                ' Try in %s seconds', next
            )
        finally:
            worker._loop.call_later(next, self.__tick, worker, next)

    async def start(self, worker, consume=True):
        self.logger.info('Start %s', self)
        await super().start()
        if consume:
            for consumer in self.consumers:
                consumer.start(worker)
            await self.worker_tick(worker)
        return self

    def __tick(self, worker, next):
        asyncio.ensure_future(
            self.worker_tick(worker, next),
            loop=worker._loop
        )
