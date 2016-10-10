from logging import getLogger

from pulsar import create_future

from .mq import BaseComponent


class ConsumerAPI(BaseComponent):

    def __init__(self, backend):
        super().__init__(backend)
        self.logger = getLogger('pulsar.%s' % self.name)
        self._closing_waiter = None

    def __str__(self):
        return self.name

    def __repr__(self):
        return '%s %s' % (self, self.backend)

    @property
    def name(self):
        return self.__class__.__name__.lower()

    @property
    def _loop(self):
        return self.backend._loop

    @property
    def manager(self):
        return self.backend.manager

    @property
    def broker(self):
        return self.backend.broker

    @property
    def pubsub(self):
        return self.backend.pubsub

    def start(self, worker, consume):
        pass

    def tick(self):
        """Called periodically by the actor.

        By default it checks for closing signal and it available do the close
        """
        if self._closing_waiter and not self._closing_waiter.done():
            self.do_close()

    def info(self):
        pass

    def rpc(self):
        pass

    def execute(self, message):
        return message

    def closing(self):
        return self._closing_waiter is not None

    def close(self, msg=None):
        """Return a Future which should be called back once the consumer
        is closed"""
        if not self.closing():
            self._closing_waiter = create_future(self._loop)
            if msg:
                self.logger.warning(msg)
            self.tick()
        return self._closing_waiter

    def do_close(self, msg=None):
        if msg:
            self.logger.warning(msg)
        self._closing_waiter.set_result(True)
