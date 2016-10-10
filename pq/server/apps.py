from pulsar import Application, MultiApp, Config
from pulsar.apps.wsgi import (WSGIServer, Router, LazyWsgi,
                              WsgiHandler, GZipMiddleware)

from .config import DEFAULT_MQ_BACKEND
from .rpc import TaskQueueRpc
from .producer import Producer
from .consumer import Consumer
from .. import __version__


class QueueApp(Application):
    """A pulsar :class:`.Application` for consuming :class:`.Task`.

    This application can also schedule periodic tasks when the
    :ref:`schedule_periodic <setting-schedule_periodic>` flag is ``True``.
    """
    backend_factory = Consumer
    name = 'tasks'
    cfg = Config(apps=('tasks',),
                 version=__version__,
                 data_store=DEFAULT_MQ_BACKEND,
                 timeout=600)
    _backend = None

    def api(self):
        return Producer(self.cfg, logger=self.logger)

    @property
    def backend(self):
        return self._backend

    async def monitor_start(self, monitor, exc=None):
        if not exc and self.cfg.workers:
            self._backend = await self._start(monitor, False)

    def monitor_task(self, monitor):
        if monitor.is_running():
            self._backend.tick(monitor)

    def monitor_stopping(self, worker, exc=None):
        if self._backend:
            backend = self._backend
            self._backend = None
            return backend.close()

    async def worker_start(self, worker, exc=None):
        if not exc:
            self._backend = await self._start(worker)
            if not worker.is_monitor():
                self._backend.bind_event('close', _close)

    def worker_stopping(self, worker, exc=None):
        if self._backend:
            return self._backend.close()

    def actorparams(self, monitor, params):
        # makes sure workers are only consuming tasks, not scheduling.
        cfg = params['cfg']
        cfg.set('schedule_periodic', False)

    def _start(self, actor, consume=True):
        return self.backend_factory(
            self.cfg,
            logger=self.logger
        ).start(actor, consume)


class PulsarQueue(MultiApp):
    """Build a multi-app consisting on a taskqueue and a JSON-RPC server.
    """
    cfg = Config('Pulsar Queue')

    def __init__(self, callable=None, **params):
        super().__init__(**params)
        self.manager = callable

    def api(self):
        return self.apps()[0].api()

    def build(self):
        yield self.new_app(QueueApp, callable=self.manager)
        wsgi = self.cfg.params.get('wsgi')
        if wsgi:
            if wsgi is True:
                wsgi = Rpc
            yield self.new_app(RpcServer,
                               prefix='rpc',
                               callable=self)


class RpcServer(WSGIServer):

    def __init__(self, callable=None, **params):
        callable = Rpc(callable.apps()[0].cfg)
        super().__init__(callable=callable, **params)


class Rpc(LazyWsgi):
    '''Default WSGI callable for the wsgi part of the application
    '''
    def __init__(self, cfg):
        self.cfg = cfg

    def setup(self, environ):
        # only post allowed by the JSON RPC handler
        api = Producer(self.cfg)
        handler = TaskQueueRpc(api)
        for consumer in api.consumers:
            rpc = consumer.rpc()
            if rpc:
                handler.putSubHandler(consumer.name, rpc)

        request = [Router('/', post=handler)]
        response = [GZipMiddleware(200)]
        return WsgiHandler(middleware=request, response_middleware=response)


def _close(backend, **kw):
    backend._loop.call_soon(backend._loop.stop)
