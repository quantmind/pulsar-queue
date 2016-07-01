"""An example of a python script which can be executed by the task queue
"""
import sys


def execute():
    """Simply return the python executable"""
    print(sys.executable)


if __name__ == '__main__':
    execute()
