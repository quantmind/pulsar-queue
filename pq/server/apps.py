from pulsar import Application, MultiApp, Config
from pulsar.apps.wsgi import (WSGIServer, Router, LazyWsgi,
                              WsgiHandler, GZipMiddleware)

from .config import DEFAULT_TASK_BACKEND
from .rpc import TaskQueueRpc
from .producer import Producer
from .consumer import Consumer
from .. import __version__


class TaskApp(Application):
    """A pulsar :class:`.Application` for consuming :class:`.Task`.

    This application can also schedule periodic tasks when the
    :ref:`schedule_periodic <setting-schedule_periodic>` flag is ``True``.
    """
    backend_factory = Consumer
    name = 'tasks'
    cfg = Config(apps=('tasks',),
                 version=__version__,
                 data_store=DEFAULT_TASK_BACKEND,
                 timeout=600)
    _backend = None

    def api(self):
        return Producer(self.cfg, logger=self.logger)

    async def monitor_start(self, monitor, exc=None):
        if not exc:
            self._backend = await self._start(monitor, False)

    def monitor_task(self, monitor):
        if monitor.is_running():
            self._backend.tick()

    def monitor_stopping(self, worker, exc=None):
        if self._backend:
            return self._backend.close()

    async def worker_start(self, worker, exc=None):
        if not exc:
            self._backend = await self._start(worker)

    def worker_stopping(self, worker, exc=None):
        if self._backend:
            return self._backend.close()

    def actorparams(self, monitor, params):
        # makes sure workers are only consuming tasks, not scheduling.
        cfg = params['cfg']
        cfg.set('schedule_periodic', False)

    def worker_info(self, worker, info=None):
        info.update(self._backend.info())

    def _start(self, actor, consume=True):
        return self.backend_factory(
            self.cfg,
            logger=self.logger
        ).start(actor, consume)


class PulsarQueue(MultiApp):
    """Build a multi-app consisting on a taskqueue and a JSON-RPC server.
    """
    cfg = Config('Pulsar Queue')

    def build(self):
        app = self.new_app(TaskApp,
                           callable=self.cfg.params.get('queue_callable'))
        yield app

        wsgi = self.cfg.params.get('wsgi')
        if wsgi:
            if wsgi is True:
                wsgi = Rpc
            yield self.new_app(WSGIServer,
                               prefix='rpc',
                               callable=wsgi(app))


class Rpc(LazyWsgi):
    '''Default WSGI callable for the wsgi part of the application
    '''
    def __init__(self, tq):
        self.tq = tq

    def setup(self, environ):
        # only post allowed by the JSON RPC handler
        api = self.tq.api()
        handler = TaskQueueRpc(self.tq.name)
        for consumer in api.consumers:
            rpc = consumer.rpc()
            if rpc:
                handler.putSubHandler(consumer.name, rpc)

        request = [Router('/', post=handler)]
        response = [GZipMiddleware(200)]
        return WsgiHandler(middleware=request, response_middleware=response)
