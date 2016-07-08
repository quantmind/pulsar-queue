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
    _task_backend_ = None

    def __init__(self, taskqueue, **kwargs):
        if not isinstance(taskqueue, str):
            taskqueue = taskqueue.name
        self.taskqueue = taskqueue
        super().__init__(**kwargs)

    ########################################################################
    #    REMOTES
    async def rpc_job_list(self, request, jobnames=None):
        '''Return the list of Jobs registered with task queue with meta
        information.

        If a list of ``jobnames`` is given, it returns only jobs
        included in the list.
        '''
        task_backend = await self._task_backend()
        return task_backend.job_list(jobnames=jobnames)

    def rpc_next_scheduled_tasks(self, request, jobnames=None):
        return self._rq(request, next_scheduled, jobnames=jobnames)

    async def rpc_queue_task(self, request, jobname=None, **kw):
        '''Queue a new ``jobname`` in the task queue.

        The task can be of any type as long as it is registered in the
        task queue registry. To check the available tasks call the
        :meth:`rpc_job_list` function.

        It returns the task :attr:`~Task.id`.
        '''
        task = await self._queue_task(request, jobname, **kw)
        return task.tojson()

    async def rpc_num_tasks(self, request):
        '''Return the approximate number of tasks in the task queue.'''
        task_backend = await self._task_backend()
        return task_backend.num_tasks()

    def task_request_parameters(self, request):
        '''**Internal function** which returns a dictionary of parameters
        to be passed to the :class:`.Task` class constructor.

        This function can be overridden to add information about
        the type of request, who made the request and so forth.
        It must return a dictionary.
        By default it returns an empty dictionary.'''
        return {}

    ########################################################################
    #    INTERNALS
    async def _task_backend(self):
        if not self._task_backend_:
            app = await get_application(self.taskqueue)
            self._task_backend_ = await app.backend.start()
        return self._task_backend_

    async def _queue_task(self, request, jobname, meta_params=None, **kw):
        if not jobname:
            raise rpc.InvalidParams('"jobname" is not specified!')
        meta_params = meta_params or {}
        meta_params.update(self.task_request_parameters(request))
        task_backend = await self._task_backend()
        result = await task_backend.queue_task(
            jobname, meta_params=meta_params, **kw)
        return result

    def _rq(self, request, func, *args, **kw):
        return pulsar.send(self.taskqueue, 'run', func, *args, **kw)


def next_scheduled(actor, jobnames=None):
    return actor.app.backend.next_scheduled(jobnames=jobnames)
