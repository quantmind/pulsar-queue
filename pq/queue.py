import time

import pulsar
from pulsar import command
from pulsar.utils.config import section_docs
from pulsar.apps.data import create_store
from pulsar.apps.ds import DEFAULT_PULSAR_STORE_ADDRESS

from .tasks import TaskBackend


DEFAULT_TASK_BACKEND = 'pulsar://%s/1' % DEFAULT_PULSAR_STORE_ADDRESS


section_docs['Task Consumer'] = '''
This section covers configuration parameters used by CPU bound type
applications such as the :ref:`distributed task queue <apps-taskqueue>` and
the :ref:`test suite <apps-test>`.'''


class TaskSetting(pulsar.Setting):
    virtual = True
    app = 'tasks'
    section = "Task Consumer"


class ConcurrentTasks(TaskSetting):
    name = "concurrent_tasks"
    flags = ["--concurrent-tasks"]
    validator = pulsar.validate_pos_int
    type = int
    default = 5
    desc = """\
        The maximum number of concurrent tasks for a worker.

        When a task worker reach this number it stops polling for more tasks
        until one or more task finish. It should only affect task queues under
        significant load.
        Must be a positive integer. Generally set in the range of 5-10.
        """


class TaskPaths(TaskSetting):
    name = "task_paths"
    validator = pulsar.validate_list
    default = []
    desc = """\
        List of python dotted paths where tasks are located.

        This parameter can only be specified during initialization or in a
        :ref:`config file <setting-config>`.
        """


class SchedulePeriodic(TaskSetting):
    name = 'schedule_periodic'
    flags = ["--schedule-periodic"]
    validator = pulsar.validate_bool
    action = "store_true"
    default = False
    desc = '''\
        Enable scheduling of periodic tasks.

        If enabled, :class:`.PeriodicJob` will produce
        tasks according to their schedule.
        '''


class TaskQueue(pulsar.Application):
    '''A pulsar :class:`.Application` for consuming :class:`.Task`.

    This application can also schedule periodic tasks when the
    :ref:`schedule_periodic <setting-schedule_periodic>` flag is ``True``.
    '''
    backend = None
    '''The :class:`.TaskBackend` for this task queue.

    Available once the :class:`.TaskQueue` has started.
    '''
    name = 'tasks'
    cfg = pulsar.Config(apps=('tasks',),
                        data_store=DEFAULT_TASK_BACKEND,
                        timeout=600)

    def monitor_start(self, monitor, exc=None):
        '''Starts running the task queue in ``monitor``.

        It calls the :attr:`.Application.callable` (if available)
        and create the :attr:`~.TaskQueue.backend`.
        '''
        if self.cfg.callable:
            self.cfg.callable()
        self.get_backend()

    def monitor_task(self, monitor):
        '''Override the :meth:`~.Application.monitor_task` callback.

        Check if the :attr:`~.TaskQueue.backend` needs to schedule new tasks.
        '''
        if self.backend and monitor.is_running():
            if self.backend.next_run <= time.time():
                self.backend.tick()

    def monitor_stopping(self, monitor, exc=None):
        if self.backend:
            self.backend.close()

    def worker_start(self, worker, exc=None):
        if not exc:
            self.get_backend().start(worker)

    def worker_stopping(self, worker, exc=None):
        if self.backend:
            return self.backend.close()

    def actorparams(self, monitor, params):
        # makes sure workers are only consuming tasks, not scheduling.
        cfg = params['cfg']
        cfg.set('schedule_periodic', False)

    def worker_info(self, worker, info=None):
        be = self.backend
        if be:
            tasks = {'concurrent': list(be.concurrent_tasks),
                     'processed': be.processed}
            info['tasks'] = tasks

    def get_backend(self):
        if self.backend is None:
            self.backend = TaskBackend(
                create_store(self.cfg.data_store),
                logger=self.logger,
                name=self.name,
                task_paths=self.cfg.task_paths,
                schedule_periodic=self.cfg.schedule_periodic,
                max_tasks=self.cfg.max_requests,
                backlog=self.cfg.concurrent_tasks)
            self.logger.debug('created %s', self.backend)
        return self.backend


@command()
def next_scheduled(request, jobnames=None):
    actor = request.actor
    return actor.app.backend.next_scheduled(jobnames)
