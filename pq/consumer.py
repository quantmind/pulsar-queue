from pulsar import create_future

from .mq import BaseComponent


class ConsumerAPI(BaseComponent):
    _closing_waiter = None

    def __str__(self):
        return self.name

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

    def start(self, worker):
        pass

    def start_scheduler(self, worker):
        pass

    def info(self):
        return {}

    def closing(self):
        return self._closing_waiter is not None

    def close(self, msg=None):
        """Return a Future which should be called back once the consumer
        is closed"""
        if not self.closing():
            self._closing_waiter = create_future(self._loop)
            if msg:
                self.logger.warning(msg)
        return self._closing_waiter

    def do_close(self):
        self.logger.warning('Closing %s', self)
        self._closing_waiter.set_result(True)
