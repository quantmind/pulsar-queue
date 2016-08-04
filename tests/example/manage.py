task_paths = ['jobs.*', 'pq.jobs']


if __name__ == '__main__':  # pragma    nocover
    from pq.api import PulsarQueue
    PulsarQueue(name='taskqueue', config=__file__).start()
