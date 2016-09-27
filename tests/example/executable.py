"""An example of a python script which can be executed by the task queue
"""
import sys


def execute():
    """Simply write the python executable
    """
    sys.stdout.write(sys.executable)


if __name__ == '__main__':
    execute()
