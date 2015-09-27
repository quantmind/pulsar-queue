from pq import TaskApp, PulsarQueue


task_paths = ['sampletasks.*']


def app():
    return TaskApp(config=__file__)


if __name__ == '__main__':  # pragma    nocover
    PulsarQueue('taskqueue', config='manage.py').start()
