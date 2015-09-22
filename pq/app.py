from pulsar import MultiApp, Config
from pulsar.apps.wsgi import (WSGIServer, Router, LazyWsgi,
                              WsgiHandler, GZipMiddleware)

from .queue import TaskQueue
from .rpc import TaskQueueRpc


class PulsarQueue(MultiApp):
    '''Build a multi-app consisting on a taskqueue and a JSON-RPC server.

    This class shows how to use the :class:`.MultiApp` utility for
    starting several :ref:`pulsar applications <apps-framework>` at once.
    '''
    cfg = Config('Pulsar Queue')

    def build(self):
        yield self.new_app(TaskQueue,
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
