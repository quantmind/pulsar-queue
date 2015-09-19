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
from pq import PulsarQueue


if __name__ == '__main__':  # pragma    nocover
    PulsarQueue('taskqueue',
                task_paths=['sampletasks.*']).start()
