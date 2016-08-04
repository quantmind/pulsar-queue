import time

from pulsar import Application, MultiApp, Config
from pulsar.apps.wsgi import (WSGIServer, Router, LazyWsgi,
                              WsgiHandler, GZipMiddleware)

from .. import __version__
from .config import DEFAULT_TASK_BACKEND
from .rpc import TaskQueueRpc
from . import TaskScheduler, TaskConsumer, TaskProducer


class TaskApp(Application):
    """A pulsar :class:`.Application` for consuming :class:`.Task`.

    This application can also schedule periodic tasks when the
    :ref:`schedule_periodic <setting-schedule_periodic>` flag is ``True``.
    """
    backend_factory = TaskConsumer
    name = 'tasks'
    cfg = Config(apps=('tasks',),
                 version=__version__,
                 data_store=DEFAULT_TASK_BACKEND,
                 timeout=600)
    _backend = None

    def api(self):
        return TaskProducer(self.cfg, logger=self.logger)

    def scheduler(self):
        return TaskScheduler(self.cfg, logger=self.logger)

    def next_scheduled(self, jobnames=None):
        if isinstance(self._backend, TaskScheduler):
            return self._backend.next_scheduled(jobnames=jobnames)

    async def monitor_start(self, monitor, exc=None):
        if not exc:
            if self.cfg.schedule_periodic:
                self.backend_factory = TaskScheduler
            elif self.cfg.workers:
                self.backend_factory = TaskProducer
            self._backend = await self._start(monitor)

    def monitor_task(self, monitor):
        if monitor.is_running() and self.cfg.schedule_periodic:
            if self._backend.next_run <= time.time():
                self._backend.tick()

    def monitor_stopping(self, worker, exc=None):
        if self._backend:
            return self._backend.close()

    async def worker_start(self, worker, exc=None):
        if not exc and not worker.is_monitor():
            self._backend = await self._start(worker)

    def worker_stopping(self, worker, exc=None):
        if self._backend:
            return self._backend.close()

    def actorparams(self, monitor, params):
        # makes sure workers are only consuming tasks, not scheduling.
        cfg = params['cfg']
        cfg.set('schedule_periodic', False)

    def worker_info(self, worker, info=None):
        info['tasks'] = self._backend.info()

    def _start(self, actor):
        return self.backend_factory(
            self.cfg,
            logger=self.logger
        ).start(actor)


class PulsarQueue(MultiApp):
    '''Build a multi-app consisting on a taskqueue and a JSON-RPC server.

    This class shows how to use the :class:`.MultiApp` utility for
    starting several :ref:`pulsar applications <apps-framework>` at once.
    '''
    cfg = Config('Pulsar Queue')

    def build(self):
        yield self.new_app(TaskApp,
                           callable=self.cfg.params.get('queue_callable'))

        wsgi = self.cfg.params.get('wsgi')
        if wsgi:
            if wsgi is True:
                wsgi = Rpc
            yield self.new_app(WSGIServer,
                               prefix='rpc',
                               callable=wsgi(self.name))


class Rpc(LazyWsgi):
    '''Default WSGI callable for the wsgi part of the application
    '''
    def __init__(self, tqname):
        self.tqname = tqname

    def setup(self, environ):
        # only post allowed by the JSON RPC handler
        request = [Router('/', post=TaskQueueRpc(self.tqname))]
        response = [GZipMiddleware(200)]
        return WsgiHandler(middleware=request, response_middleware=response)
