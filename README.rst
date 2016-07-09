|pulsar-queue|

:Badges: |license|  |pyversions| |status| |pypiversion|
:Master CI: |master-build| |coverage-master|
:Downloads: http://pypi.python.org/pypi/pulsar-queue
:Source: https://github.com/quantmind/pulsar-queue
:Mailing list: `google user group`_
:Design by: `Quantmind`_ and `Luca Sbardella`_
:Platforms: Linux, OSX, Windows. Python 3.5 and above
:Keywords: server, asynchronous, concurrency, actor, process, queue, tasks, redis


.. |pypiversion| image:: https://badge.fury.io/py/pulsar-queue.svg
    :target: https://pypi.python.org/pypi/pulsar-queue
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
.. |pulsar-queue| image:: https://pulsar.fluidily.com/assets/queue/pulsar-queue-banner-400-width.png
   :target: https://github.com/quantmind/pulsar-queue


Asynchronous server for consuming asynchronous IO tasks, green IO tasks,
blocking IO tasks and long running CPU bound tasks.

* Fully configurable
* Consumers poll tasks from distributed message brokers (redis broker implemented)
* Publish/subscribe for real-time event and logging (redis pub/sub backend)
* Can schedule tasks when run as a scheduler (``--schedule-periodic`` flag)
* Build on top of pulsar_ and asyncio_


.. contents:: **CONTENTS**


Four steps tutorial
========================

1 - Create a script
----------------------

A simple python file which runs your application:

.. code::

    vim manage.py


.. code:: python

    from pq.api import TaskApp


    task_paths = ['sampletasks.*', 'pq.jobs']


    def app():
        return TaskApp(config=__file__)

    if __name__ == '__main__':
        app().start()


2 - Implement Jobs
---------------------

Create the modules where Jobs_ are implemented.
It can be a directory containing several submodules.

.. code::

    mkdir sampletasks
    cd sampletasks
    vim mytasks.py

.. code:: python

    import asyncio
    import time

    from pq import api


    @api.job()
    def addition(self, a=0, b=0):
        return a + b


    @api.job(concurrency=api.ASYNC_IO)
    async def asynchronous(self, lag=1):
        start = time.time()
        await asyncio.sleep(lag)
        return time.time() - start


3 - Run the server
---------------------

Run the server with two task consumers (pulsar actors).

**NOTE**: Make sure you have Redis server up and running before you start the queue.

.. code::

    python manage.py -w 2

4 - Queue tasks
---------------------

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

API
=============

Tasks backend
-----------------

The tasks backend is obtained from the Task application ``backend`` attribute:

.. code:: python

    from pq.api import TaskApp

    tasks = TaskApp(...).backend


* tasks. **queue_task** (*jobname*, *\*args*, *\*\*kwargs*)

    Queue a task and return a **TaskFuture** which is resolved once the task has finished.
    It is possible to obtain a task future resolved when the task has been queued, rather than finished, by passing the **callback=False** parameter:

    .. code:: python

        task = await tasks.queue_task(..., callback=False)
        task.status_string  # QUEUED

* tasks. **queue_task_local** (*jobname*, *\*args*, *\*\*kwargs*)

    Queue a job in the local task queue. The local task queue is processed by the same server instance. It is equivalent to execute:

    .. code:: python

        task = await tasks.queue_task(..., queue=tasks.node_name)
        task.queue  # tasks.node_name


* tasks. **execute_task** (*jobname*, *\*args*, *\*\*kwargs*)

    Execute a task immediately, it does not put the task in the task queue.
    This method is useful for debugging and testing. It is equivalent to execute:

    .. code:: python

        task = await tasks.queue_task(..., queue=False)
        task.queue          # None
        task.status_string  # SUCCESS


* tasks. **queues** ()

    Return the list of queue names the backend is subscribed. This list is not empty when the backend is a task consumer.

* tasks. **job_list** (*jobnames* = *None*)

    Returns a list of ``job_name``, ``job_description`` tuples. The ``job_name`` is a string which must be used as the **jobname** parameter when executing or queing tasks. The ``job_description`` is a dictionary containing metadata and documentation for the job. Example:

    .. code:: python

        jobs = dict(tasks.job_lits())
        jobs['execute.python']
        # {
        #   'type': 'regular',
        #   'concurrency': 'asyncio',
        #   'doc_syntax': 'markdown',
        #   'doc': 'Execute arbitrary python code on a subprocess ... '
        # }


The Job class
-----------------

The **Job** class is how task factories are implemented and added to the
tasks backend registry. When writing a new **Job** one can either subclass:

.. code:: python

    import asyncio

    class AsyncSleep(api.Job):
        concurrency api.ASYNC_IO

        async def __call__(self, lag=1):
            await asyncio.sleep(lag)


or use the less verbose **job** decorator:

.. code:: python

    @api.job(concurrency=api.ASYNC_IO
    async def asyncsleep(self, lag=1):
        await asyncio.sleep(lag)


In either cases the ``self`` parameter is an instance of the **Job** class.

* job. **backend**

    The tasks backend that is processing this Job run

* job. **task**

    The Task_ instance associated with this job run

* job. **http**

    Best possible HTTP session handler for the job concurrency mode.

* job. **queue_task** (*jobname*, *\*args*, *\*\*kwargs*)

    Queue a new job. It is equivalent to:

    .. code:: python

        meta_params = {'from_task': self.task.id}
        self.backend.queue_task(..., meta_params=meta_params)

The Task
-----------

A task contains the metadata information of a job run and it is exchanged between task producers and task consumers via a distributed task queue.


Task States
-----------------

A Task_ can have one of the following ``task.status``:

* ``QUEUED = 6`` a task queued but not yet executed.
* ``STARTED = 5`` a task where execution has started.
* ``RETRY = 4`` a task is retrying calculation.
* ``REVOKED = 3`` the task execution has been revoked (or timed-out).
* ``FAILURE = 2`` task execution has finished with failure.
* ``SUCCESS = 1`` task execution has finished with success.


**FULL_RUN_STATES**

The set of states for which a Task_ has run: ``FAILURE`` and ``SUCCESS``


**READY_STATES**

The set of states for which a Task_ has finished: ``REVOKED``, ``FAILURE`` and ``SUCCESS``


Configuration
------------------

There are several parameters you can use to twick the way the task queue works.

* **concurrent_tasks** (``--concurrent-tasks 5``)

    The maximum number of concurrent tasks for a given worker in task consumer server.

* **schedule_periodic** (``--schedule-periodic``)

    When ``True``, the task application can schedule periodic Jobs_.
    Usually, only one running server is responsible for
    scheduling tasks.


Tasks Concurrency
======================

A task can run in one of four ``concurrency`` modes.
If not specified by the ``Job``, the concurrency mode is given by the
``default_task_concurrency`` parameter whch can be specified in the ``config`` file or in the command line.

ASYNC_IO
-----------

The asynchronous IO mode is associated with tasks which return
an asyncio Future or a coroutine. These tasks run concurrently
in the worker event loop.
An example can be a Job to scrape web pages and create new tasks to process the html

.. code:: python

    @api.job(concurrency=api.ASYNC_IO)
    async def scrape(self, url=None):
        assert url, "url is required"
        request = await self.http.get(url)
        html = request.text()
        task = self.queue_task('process.html', html=html, callback=False)
        return task.id

GREEN_IO
----------

The green IO mode is associated with tasks that runs on a child greenlet.
This can be useful when using applications which use the greenlet_
library for implicit asynchronous behaviour.

THREAD_IO
-------------

It assumes the task performs blocking IO operations
which make it suitable to be run in the event loop executor.
You can use this model for most blocking operation unless

* Long running CPU bound
* The operation does not release the GIL

CPUBOUND
------------

It assumes the task performs blocking CPU bound operations.
These tasks are run on sub-processes.

Configure
=================

It is possible to enhance the task queue by passing an application ``callable``
during initialisation (usually a class or an instance factory).
This callable must be picklable and should return an object which can implement one or
more methods which override the ``beckend`` implementation.

For example:

.. code:: python

    class Application:

        def __init__(self, backend):
            self.backend = backend

        async def store_task(self, task):
            """Store task into a backend database"""
            ...

    tq = TaskApp(Application, ...)


The application callable is invoked when the backend handler is initialised
(on each consumer and in the scheduler).

Changelog
==============

* `Versions 0.3 <https://github.com/quantmind/pulsar-queue/blob/master/docs/history/0.3.md>`_
* `Versions 0.2 <https://github.com/quantmind/pulsar-queue/blob/master/docs/history/0.2.md>`_
* `Versions 0.1 <https://github.com/quantmind/pulsar-queue/blob/master/docs/history/0.1.md>`_

License
=============
This software is licensed under the BSD 3-clause License. See the LICENSE
file in the top distribution directory for the full license text. Logo designed by Ralf Holzemer,
`creative common license`_.


.. _`google user group`: https://groups.google.com/forum/?fromgroups#!forum/python-pulsar
.. _`Luca Sbardella`: http://lucasbardella.com
.. _`Quantmind`: http://quantmind.com
.. _`creative common license`: http://creativecommons.org/licenses/by-nc/3.0/
.. _pulsar: https://github.com/quantmind/pulsar
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _Jobs: #the-job-class
.. _Task: #the-task
