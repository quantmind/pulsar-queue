from pulsar.api import Setting
from pulsar.utils.config import (
    validate_list, validate_bool, validate_pos_float
)
from pulsar.utils.importer import module_attribute

from ..utils.serializers import serializers


DEFAULT_MQ_BACKEND = 'redis://127.0.0.1:6379/7?namespace=pq'


def constant_or_function(value):
    try:
        value = int(value)
    except ValueError:
        return module_attribute(value)
    else:
        return Constant(value)


class Constant:

    def __init__(self, value):
        self.value = value

    def __call__(self, cfg):
        return self.value


class TaskSetting(Setting):
    virtual = True
    app = 'tasks'
    section = "Task Consumer"


class ConsumersPaths(TaskSetting):
    name = "consumers"
    validator = validate_list
    default = ['pq.api:Tasks']
    desc = """\
        List of python dotted paths where Consumer are implemented.

        This parameter can only be specified during initialization or in a
        :ref:`config file <setting-config>`.
        """


class MessageBroker(TaskSetting):
    name = 'message_broker'
    flags = ['--message-broker']
    meta = "CONNECTION STRING"
    default = ''
    desc = """\
        Connection string to message broker.
    """


class ConcurrentTasks(TaskSetting):
    name = "concurrent_tasks"
    flags = ["--concurrent-tasks"]
    default = 'pq.tasks.concurrency:linear'
    validator = constant_or_function
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
    validator = validate_list
    desc = """\
        List of queues to consume
        """


class TaskPoolTimeout(TaskSetting):
    name = "task_pool_timeout"
    flags = ["--task-pool-timeout"]
    default = 0.1
    validator = validate_pos_float
    type = int
    desc = """\
        Timeout for asynchronously polling tasks from the queues when
        no concurrent tasks in consumer
        """


class TaskPoolTimeoutMax(TaskSetting):
    name = "task_pool_timeout_max"
    flags = ["--task-pool-timeout-max"]
    default = 2
    validator = validate_pos_float
    type = int
    desc = """\
        Timeout for asynchronously polling tasks from the queues when
        concurrent tasks are at max_concurrency level
        """


class TaskPaths(TaskSetting):
    name = "task_paths"
    validator = validate_list
    default = []
    desc = """\
        List of python dotted paths where tasks are located.

        This parameter can only be specified during initialization or in a
        :ref:`config file <setting-config>`.
        """


class SchedulePeriodic(TaskSetting):
    name = 'schedule_periodic'
    flags = ["--schedule-periodic"]
    validator = validate_bool
    action = "store_true"
    default = False
    desc = '''\
        Enable scheduling of periodic tasks.

        If enabled, :class:`.PeriodicJob` will produce
        tasks according to their schedule.
        '''


class MessageSerializer(TaskSetting):
    name = 'message_serializer'
    flags = ["--message-serializer"]
    choices = tuple(serializers)
    default = tuple(serializers)[0]
    desc = '''\
        Message serializer
        '''
