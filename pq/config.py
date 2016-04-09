import pulsar

from .consumer import ConsumerMixin
from .scheduler import SchedulerMixin
from .producer import TaskProducer


DEFAULT_TASK_BACKEND = 'redis://127.0.0.1:6379/7'


class TaskSetting(pulsar.Setting):
    virtual = True
    app = 'tasks'
    section = "Task Consumer"


class DefaultTaskConcurrency(TaskSetting):
    name = "default_task_concurrency"
    flags = ["--default-task-concurrency"]
    choices = ('asyncio', 'greenio', 'thread', 'process')
    default = 'greenio'
    desc = """\
        Default concurrency for tasks
        """


class BroadCastPrefix(TaskSetting):
    name = "task_queue_prefix"
    flags = ["--task_queue_prefix"]
    default = ''
    desc = """\
        Prefix for all broadcast channels and queues

        Messages emitted in these channels by the application will be:

        <prefix>_task_queued
        <prefix>_task_started
        <prefix>_task_finished

        if the prefix is set. Same for queues.
        """


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
    default = ['pulsarqueue']
    validator = pulsar.validate_list
    desc = """\
        List of queues to consume
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


class TaskScheduler(SchedulerMixin, TaskProducer):
    pass


class TaskConsumer(ConsumerMixin, TaskProducer):
    pass
