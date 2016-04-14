task_paths = ['sampletasks.*']


def app():
    from pq.api import TaskApp
    return TaskApp(config=__file__)


if __name__ == '__main__':  # pragma    nocover
    from pq.api import PulsarQueue
    PulsarQueue('taskqueue', config='manage.py').start()
