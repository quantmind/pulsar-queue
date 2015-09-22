'''Asynchronous task queues'''
import os


VERSION = (0, 1, 0, 'alpha', 0)

__version__ = '.'.join((str(v) for v in VERSION))
__author__ = "Luca Sbardella"
__contact__ = "luca@quantmind.com"
__homepage__ = "https://github.com/quantmind/pulsar-queue"


if os.environ.get('pulsar_queue_setup') != 'yes':
    from pulsar.utils.version import get_version

    from .app import PulsarQueue            # noqa
    from .tasks import nice_task_message    # noqa
    from .models import *                   # noqa

    __version__ = get_version(VERSION)
