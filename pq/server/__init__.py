from .consumer import ConsumerMixin
from .scheduler import SchedulerMixin
from .producer import TaskProducer


class TaskScheduler(SchedulerMixin, TaskProducer):
    pass


class TaskConsumer(ConsumerMixin, TaskProducer):
    pass
