import time

import pulsar
from pulsar import command
from pulsar.utils.config import section_docs
from pulsar.apps.ds import DEFAULT_PULSAR_STORE_ADDRESS

from .consumer import ConsumerMixin
from .scheduler import SchedulerMixin
from .producer import TaskProducer


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


class DefaultQueue(TaskSetting):
    name = "default_task_queue"
    flags = ["--default-task-queue"]
    default = 'pulsarqueue'
    desc = """\
        Default queue name when not specified by the Job class
        """


class TaskQueues(TaskSetting):
    name = 'task_queues'
    default = {}
    validator = pulsar.validate_dict
    desc = """\
        Advance configurator for task queues

        A dictionary matching task queues with
        """


class TaskPoolTimeout(TaskSetting):
    name = "task_pool_timeout"
    flags = ["--task-pool-timeout"]
    default = 2
    type = int
    desc = """\
        Timeout for asynchronously poolling tasks from the queues
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


class TaskScheduler(TaskProducer, SchedulerMixin):
    pass


class TaskConsumer(TaskProducer, ConsumerMixin):
    pass


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
        self.backend = TaskScheduler(self.cfg, logger=self.logger)

    def monitor_task(self, monitor):
        '''Override the :meth:`~.Application.monitor_task` callback.

        Check if the :attr:`~.TaskQueue.backend` needs to schedule new tasks.
        '''
        if monitor.is_running() and self.backend:
            if self.backend.next_run <= time.time():
                self.backend.tick()

    def monitor_stopping(self, monitor, exc=None):
        if self.backend:
            return self.backend.close()

    def worker_start(self, worker, exc=None):
        if not exc:
            queue = self.cfg.params.get('task_queue')
            self.backend = TaskConsumer(self.cfg, queue=queue,
                                        logger=self.logger)
            self.backend.start(worker)

    def worker_stopping(self, worker, exc=None):
        if self.backend:
            self.backend.close()

    def actorparams(self, monitor, params):
        # makes sure workers are only consuming tasks, not scheduling.
        cfg = params['cfg']
        cfg.set('schedule_periodic', False)
        cfg.update(self.choose_queue(monitor))

    def worker_info(self, worker, info=None):
        if self.backend:
            info['tasks'] = self.backend.info()

    def choose_queue(self, monitor):
        queues = self.cfg.task_queues
        if not queues:
            return {'task_queue': self.cfg.default_task_queue,
                    'concurrent_tasks': self.cfg.concurrent_tasks}
        else:
            raise NotImplementedError


@command()
def next_scheduled(request, jobnames=None):
    actor = request.actor
    return actor.app.backend.next_scheduled(jobnames)
