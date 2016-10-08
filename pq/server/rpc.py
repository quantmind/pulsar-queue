import pulsar
from pulsar import get_application
from pulsar.apps import rpc


class TaskQueueRpc(rpc.JSONRPC):
    '''A :class:`.JSONRPC` mixin for communicating with  a :class:`.TaskQueue`.

    To use it, you need to have an :ref:`RPC application <apps-rpc>`
    and a :ref:`task queue <apps-taskqueue>` application installed in the
    :class:`.Arbiter`.

    :parameter taskqueue: instance or name of the :class:`.TaskQueue`
        application which exposes the remote procedure calls.

    '''
    _task_api_ = None

    def __init__(self, taskqueue, **kwargs):
        if not isinstance(taskqueue, str):
            taskqueue = taskqueue.name
        self.taskqueue = taskqueue
        super().__init__(**kwargs)

    def rq(self, request, func, *args, **kw):
        return pulsar.send(self.taskqueue, 'run', func, *args, **kw)

    async def api(self):
        if not self._task_api_:
            app = await get_application(self.taskqueue)
            self._task_api_ = await app.api().start()
        return self._task_api_
