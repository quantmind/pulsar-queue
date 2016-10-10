import pulsar
from pulsar.apps import rpc


class TaskQueueRpc(rpc.JSONRPC):
    '''A :class:`.JSONRPC` mixin for communicating with  a :class:`.TaskQueue`.

    To use it, you need to have an :ref:`RPC application <apps-rpc>`
    and a :ref:`task queue <apps-taskqueue>` application installed in the
    :class:`.Arbiter`.

    :parameter taskqueue: instance or name of the :class:`.TaskQueue`
        application which exposes the remote procedure calls.

    '''
    def __init__(self, api, **kwargs):
        self._api_ = (api,)
        super().__init__(**kwargs)

    async def rq(self, request, func, *args, **kw):
        api = await self.api()
        result = await pulsar.send(api.cfg.name, 'run', func, *args, **kw)
        return result

    async def api(self):
        if isinstance(self._api_, tuple):
            self._api_ = await self._api_[0].start()
        return self._api_
