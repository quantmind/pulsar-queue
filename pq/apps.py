import time

from pulsar import Application, MultiApp, Config
from pulsar.apps.wsgi import (WSGIServer, Router, LazyWsgi,
                              WsgiHandler, GZipMiddleware)
from pulsar.utils.log import lazyproperty

from . import __version__
from .config import TaskScheduler, TaskConsumer, DEFAULT_TASK_BACKEND
from .rpc import TaskQueueRpc


class TaskApp(Application):
    """A pulsar :class:`.Application` for consuming :class:`.Task`.

    This application can also schedule periodic tasks when the
    :ref:`schedule_periodic <setting-schedule_periodic>` flag is ``True``.
    """
    backend = None
    '''The :class:`.TaskBackend` for this task queue.

    Available once the :class:`.TaskQueue` has started.
    '''
    name = 'tasks'
    cfg = Config(apps=('tasks',),
                 version=__version__,
                 data_store=DEFAULT_TASK_BACKEND,
                 timeout=600)

    @lazyproperty
    def backend(self):
        queues = getattr(self, 'queues', None)
        app = None
        if self.cfg.callable:
            app = self.cfg.callable()
        if queues:
            return TaskConsumer(self.cfg, queues=queues, app=app,
                                logger=self.logger)
        else:
            return TaskScheduler(self.cfg, app=app, logger=self.logger)

    def queue_task(self, job_name, **kwargs):
        """Queue a job via the backend
        """
        return self.backend.queue_task(job_name, **kwargs)

    def execute_task(self, job_name, **kwargs):
        """Execute a job via the backend
        """
        return self.backend.execute_task(job_name, **kwargs)

    def monitor_start(self, monitor, exc=None):
        '''Starts running the task queue in ``monitor``.

        It calls the :attr:`.Application.callable` (if available)
        and create the :attr:`~.TaskQueue.backend`.
        '''
        return self.backend.ready()

    def monitor_task(self, monitor):
        '''Override the :meth:`~.Application.monitor_task` callback.

        Check if the :attr:`~.TaskQueue.backend` needs to schedule new tasks.
        '''
        if monitor.is_running():
            if self.backend.next_run <= time.time():
                self.backend.tick()

    def monitor_stopping(self, monitor, exc=None):
        return self.backend.close()

    def worker_start(self, worker, exc=None):
        if not exc and not worker.is_monitor():
            self.queues = self.cfg.task_queues
            self.backend.start(worker)

    def worker_stopping(self, worker, exc=None):
        self.backend.close()

    def actorparams(self, monitor, params):
        # makes sure workers are only consuming tasks, not scheduling.
        cfg = params['cfg']
        cfg.set('schedule_periodic', False)

    def worker_info(self, worker, info=None):
        info['tasks'] = self.backend.info()


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
