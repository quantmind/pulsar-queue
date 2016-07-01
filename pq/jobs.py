"""Useful Job for the task queue.

Include this file in the ``task_paths`` list if you need them
"""
import sys
import os
import tempfile

from pq.api import job, ASYNC_IO


@job(concurrency=ASYNC_IO)
async def execute_python(self, code=None):
    """Execute arbitrary python code on a subprocess
    """
    assert isinstance(code, str), "code must be a string"
    fp, path = tempfile.mkstemp(suffix='.py', text=True)
    try:
        with open(path, 'w') as fp:
            fp.write(code)

        command = '%s %s' % (sys.executable, path)
        result = await self.shell(command)
    finally:
        os.remove(path)
    return result
