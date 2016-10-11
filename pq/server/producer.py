import platform
import asyncio

from pulsar import new_event_loop, ensure_future, EventHandler
from pulsar.apps.data import create_store
from pulsar.apps.greenio import GreenHttp
from pulsar.apps.http import HttpClient
from pulsar.utils.importer import module_attribute

from ..utils import concurrency
from ..backends import brokers
from ..mq import Manager
from ..pubsub import PubSub


class Producer(EventHandler):
    """Produce tasks by queuing them

    Abstract base class for both task schedulers and task consumers
    """
    app = None
    ONE_TIME_EVENTS = ('close',)

    def __init__(self, cfg, *, logger=None, **kw):
        loop = cfg.params.pop('loop', None)
        store = create_store(cfg.data_store, loop=loop)
        super().__init__(loop=store._loop)
        self.cfg = cfg
        self._logger = logger
        self._closing_waiter = None
        if not cfg.message_broker:
            broker = store
        else:
            broker = create_store(cfg.message_broker, loop=loop)
        self.manager = (self.cfg.callable or Manager)(self)
        self.pubsub = PubSub(self, store)
        self.broker = brokers.get(broker.name)(self, broker)
        self.http = self.manager.http()
        self.green_pool = self.manager.green_pool()
        self.consumers = []
        for consumer_path in self.cfg.consumers:
            consumer = module_attribute(consumer_path)(self)
            self.consumers.append(consumer)
            setattr(self, consumer.name, consumer)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return 'producer <%s>' % self.broker

    @property
    def node_name(self):
        return platform.node().lower()

    @property
    def is_consumer(self):
        return False

    async def start(self):
        await self.pubsub.start()
        return self

    def tick(self, monitor):
        pass

    def info(self):
        for consumer in self.consumers:
            info = consumer.info()
            if info:
                yield consumer.name, info

    def lock(self, name, **kwargs):
        """aquire a distributed global lock for ``name``
        """
        return self.pubsub.lock('lock-%s' % name, **kwargs)

    def http_sessions(self, model=None):
        """Return an HTTP session handler for a given concurrency model
        """
        if model == concurrency.THREAD_IO:
            return HttpClient(loop=new_event_loop())
        elif model == concurrency.ASYNC_IO:
            return self.http
        else:
            return GreenHttp(self.http)

    def on_events(self, callback):
        return self.pubsub.on_events(callback)

    def remove_event_callback(self, callback):
        return self.pubsub.remove_event_callback(callback)

    def queue(self, message, callback=True):
        return self.broker.queue(message, callback=callback)

    def execute(self, message):
        consumer = message.consumer()
        if consumer:
            return getattr(self, consumer).execute(message)
        return message

    def closing(self):
        return self._closing_waiter is not None

    def close(self, msg=None):
        '''Close this :class:`.TaskBackend`.

        Invoked by the :class:`.Actor` when stopping.
        '''
        if not self._closing_waiter:
            if msg:
                self.logger.warning(msg)
            closing = []
            for consumer in self.consumers:
                result = consumer.close()
                if not result.done():
                    closing.append(result)

            self._closing_waiter = ensure_future(
                _close(self, closing, self._loop),
                loop=self._loop
            )
        return self._closing_waiter


async def _close(self, closing, loop):
    if closing:
        await asyncio.gather(*closing, loop=loop)
    self.manager.close()
    self.fire_event('close')
