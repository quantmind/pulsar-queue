'''
To run the server type::

    python manage.py

Open a new shell and launch python and type::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://localhost:8060')
    >>> p.ping()
    'pong'
    >>> p.calc.add(3,4)
    7.0
    >>>
'''
from pq import TaskApp, PulsarQueue


task_paths = ['sampletasks.*']


def app():
    return TaskApp(config='manage.py')


if __name__ == '__main__':  # pragma    nocover
    PulsarQueue('taskqueue', config='manage.py').start()
