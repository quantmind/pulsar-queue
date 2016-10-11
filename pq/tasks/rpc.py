from pulsar.apps import rpc


class TasksRpc(rpc.JSONRPC):
    '''A :class:`.JSONRPC` mixin for communicating with  a :class:`.TaskQueue`.

    To use it, you need to have an :ref:`RPC application <apps-rpc>`
    and a :ref:`task queue <apps-taskqueue>` application installed in the
    :class:`.Arbiter`.

    :parameter taskqueue: instance or name of the :class:`.TaskQueue`
        application which exposes the remote procedure calls.

    '''
    async def rpc_job_list(self, request, jobnames=None):
        '''Return the list of Jobs registered with task queue with meta
        information.

        If a list of ``jobnames`` is given, it returns only jobs
        included in the list.
        '''
        api = await self.parent.api()
        return api.tasks.job_list(jobnames=jobnames)

    def rpc_next_scheduled_tasks(self, request, jobnames=None):
        return self.parent.rq(request, next_scheduled, jobnames=jobnames)

    async def rpc_queue(self, request, jobname=None, **kw):
        '''Queue a new ``jobname`` in the task queue.

        The task can be of any type as long as it is registered in the
        task queue registry. To check the available tasks call the
        :meth:`rpc_job_list` function.

        It returns the task :attr:`~Task.id`.
        '''
        task = await self._queue(request, jobname, **kw)
        return task.tojson()

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
    async def _queue(self, request, jobname, meta_params=None, **kw):
        if not jobname:
            raise rpc.InvalidParams('"jobname" is not specified!')
        meta_params = meta_params or {}
        meta_params.update(self.task_request_parameters(request))
        api = await self.parent.api()
        result = await api.tasks.queue(jobname, meta_params=meta_params, **kw)
        return result


def next_scheduled(actor, jobnames=None):
    backend = actor.app.backend
    return backend.tasks.next_scheduled(jobnames=jobnames)
