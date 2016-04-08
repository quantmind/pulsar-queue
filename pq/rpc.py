import pulsar
from pulsar import get_application
from pulsar.apps import rpc

from .producer import Task


def task_to_json(task):
    if task:
        if isinstance(task, (list, tuple)):
            task = [task_to_json(t) for t in task]
        elif isinstance(task, Task):
            task = task.to_json()
    return task


class TaskQueueRpc(rpc.JSONRPC):
    '''A :class:`.JSONRPC` mixin for communicating with  a :class:`.TaskQueue`.

    To use it, you need to have an :ref:`RPC application <apps-rpc>`
    and a :ref:`task queue <apps-taskqueue>` application installed in the
    :class:`.Arbiter`.

    :parameter taskqueue: instance or name of the :class:`.TaskQueue`
        application which exposes the remote procedure calls.

    '''
    _task_backend = None

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
        task_backend = await self.task_backend()
        return task_backend.job_list(jobnames=jobnames)

    def rpc_next_scheduled_tasks(self, request, jobnames=None):
        return self._rq(request, 'next_scheduled', jobnames=jobnames)

    async def rpc_queue_task(self, request, jobname=None, **kw):
        '''Queue a new ``jobname`` in the task queue.

        The task can be of any type as long as it is registered in the
        task queue registry. To check the available tasks call the
        :meth:`rpc_job_list` function.

        It returns the task :attr:`~Task.id`.
        '''
        result = await self.queue_task(request, jobname, **kw)
        return task_to_json(result)

    async def rpc_wait_for_task(self, request, id=None, timeout=None):
        '''Wait for a task to have finished.

        :param id: the id of the task to wait for.
        :param timeout: optional timeout in seconds.
        :return: the json representation of the task once it has finished.
        '''
        if id:
            task_backend = await self.task_backend()
            result = await task_backend.wait_for_task(id, timeout=timeout)
            return task_to_json(result)

    async def rpc_num_tasks(self, request):
        '''Return the approximate number of tasks in the task queue.'''
        task_backend = await self.task_backend()
        return task_backend.num_tasks()

    ########################################################################
    #    INTERNALS
    async def task_backend(self):
        if not self._task_backend:
            app = await get_application(self.taskqueue)
            self._task_backend = app.get_backend()
        return self._task_backend

    async def queue_task(self, request, jobname, meta_data=None, **kw):
        if not jobname:
            raise rpc.InvalidParams('"jobname" is not specified!')
        meta_data = meta_data or {}
        meta_data.update(self.task_request_parameters(request))
        task_backend = await self.task_backend()
        result = await task_backend.queue_task(jobname, meta_data, **kw)
        return result

    def task_request_parameters(self, request):
        '''**Internal function** which returns a dictionary of parameters
        to be passed to the :class:`.Task` class constructor.

        This function can be overridden to add information about
        the type of request, who made the request and so forth.
        It must return a dictionary.
        By default it returns an empty dictionary.'''
        return {}

    def _rq(self, request, action, *args, **kw):
        return pulsar.send(self.taskqueue, action, *args, **kw)
