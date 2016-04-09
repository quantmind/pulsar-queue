|pulsar-queue|

Asynchronous task queue for consuming asynchronous IO tasks, green IO tasks,
blocking IO tasks and long running CPU bound tasks.

:Badges: |license|  |pyversions| |status| |downloads|
:Master CI: |master-build| |coverage-master|
:Dev CI: |dev-build| |coverage-dev|
:Documentation: https://github.com/quantmind/pulsar-queue/blob/master/docs/index.md
:Downloads: http://pypi.python.org/pypi/pulsar-queue
:Source: https://github.com/quantmind/pulsar-queue
:Mailing list: `google user group`_
:Design by: `Quantmind`_ and `Luca Sbardella`_
:Platforms: Linux, OSX, Windows. Python 3.5 and above
:Keywords: server, asynchronous, concurrency, actor, process, queue, tasks, redis


.. |pyversions| image:: https://img.shields.io/pypi/pyversions/pulsar-queue.svg
  :target: https://pypi.python.org/pypi/pulsar-queue
.. |license| image:: https://img.shields.io/pypi/l/pulsar-queue.svg
  :target: https://pypi.python.org/pypi/pulsar-queue
.. |status| image:: https://img.shields.io/pypi/status/pulsar-queue.svg
  :target: https://pypi.python.org/pypi/pulsar-queue
.. |downloads| image:: https://img.shields.io/pypi/dd/pulsar-queue.svg
  :target: https://pypi.python.org/pypi/pulsar-queue
.. |master-build| image:: https://img.shields.io/travis/quantmind/pulsar-queue/master.svg
  :target: https://travis-ci.org/quantmind/pulsar-queue
.. |dev-build| image:: https://img.shields.io/travis/quantmind/pulsar-queue/dev.svg
  :target: https://travis-ci.org/quantmind/pulsar-queue?branch=dev
.. |coverage-master| image:: https://coveralls.io/repos/github/quantmind/pulsar-queue/badge.svg?branch=master
  :target: https://coveralls.io/github/quantmind/pulsar-queue?branch=master
.. |coverage-dev| image:: https://coveralls.io/repos/github/quantmind/pulsar-queue/badge.svg?branch=dev
  :target: https://coveralls.io/github/quantmind/pulsar-queue?branch=dev
.. |pulsar-queue| image:: https://quantmind-public.s3.amazonaws.com/pulsar/pulsar_colored_logo_only.svg
   :width: 200 px
   :target: https://github.com/quantmind/pulsar-queue/blob/master/docs/index.md


Four steps tutorial
------------------------

**1 - Create a script which runs your application**:

.. code::

    vim manage.py


.. code:: python

    from pq.api import TaskApp


    task_paths = ['sampletasks.*']


    def app():
        return TaskApp(config=__file__)

    if __name__ == '__main__':
        app().start()


**2 - Create the modules where Jobs are implemented**

It can be a directory containing several submodules.

.. code::

    mkdir sampletasks
    cd sampletasks
    vim mytasks.py

.. code:: python

    import asyncio
    import time

    from pq import api


    class Addition(api.Job):

        def __call__(self, a=0, b=0):
            return a + b


    class Asynchronous(api.Job):
        concurrency = api.ASYNC_IO

        async def __call__(self, lag=1):
            start = time.time()
            await asyncio.sleep(lag)
            return time.time() - start

**3 - Run the server**

Run the server with two task consumers (pulsar actors).

NOTE: Make sure you have Redis server up and running before you start the queue.

.. code::

    python manage.py -w 2

**4 - Queue tasks**

Launch a python shell and play with the api

.. code:: python

    >>> from manage import app
    >>> api = app().backend
    >>> task = api.queue_task('addition', a=4, b=6)
    >>> task
    <TaskFuture pending ID=i26ad5c14c5bb422e87b0f7ccbce5ba06>
    >>> task = task.wait()
    task.addition<i24ab99ddf2744902a375e039790dcbc4><SUCCESS>
    >>> task.result
    10
    >>> task.status_string
    'SUCCESS'

License
=============
This software is licensed under the BSD 3-clause License. See the LICENSE
file in the top distribution directory for the full license text. Logo designed by Ralf Holzemer,
`creative common license`_.


.. _`google user group`: https://groups.google.com/forum/?fromgroups#!forum/python-pulsar
.. _`Luca Sbardella`: http://lucasbardella.com
.. _`Quantmind`: http://quantmind.com
.. _`creative common license`: http://creativecommons.org/licenses/by-nc/3.0/
