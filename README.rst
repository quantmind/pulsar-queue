|pulsar-queue|

Asynchronous task queue for consuming asynchronous IO tasks, green IO tasks,
blocking IO tasks and long running CPU bound tasks.

:Master CI: |master-build|_ |coverage-master|
:Documentation: https://github.com/quantmind/pulsar-queue/blob/master/docs/index.md
:Downloads: http://pypi.python.org/pypi/pulsar-queue
:Source: https://github.com/quantmind/pulsar-queue
:Mailing list: `google user group`_
:Design by: `Quantmind`_ and `Luca Sbardella`_
:Platforms: Linux, OSX, Windows. Python 3.4 and above
:Keywords: server, asynchronous, concurrency, actor, process, queue, tasks, redis


.. |pulsar-queue| image:: https://quantmind-public.s3.amazonaws.com/pulsar/pulsar_colored_logo_only.svg
   :width: 200 px
   :target: https://github.com/quantmind/pulsar-queue/blob/master/docs/index.md
.. |master-build| image:: https://travis-ci.org/quantmind/pulsar-queue.svg?branch=master
.. _master-build: http://travis-ci.org/quantmind/pulsar-queue
.. |coverage-master| image:: https://coveralls.io/repos/quantmind/pulsar-queue/badge.svg?branch=master&service=github
  :target: https://coveralls.io/github/quantmind/pulsar-queue?branch=master


Four steps tutorial
------------------------

**1 - Create a script which runs your application**:

.. code::

    vim manage.py


.. code:: python

    from pq import TaskApp


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
    
    import pq
    
    
    class Addition(pq.Job):
    
        def __call__(self, a=0, b=0):
            return a + b


    class Asynchronous(pq.Job):
        concurrency = pq.ASYNC_IO

        def __call__(self, lag=1):
            start = time.time()
            yield from asyncio.sleep(lag)
            return time.time() - start

**3 - Run the server**

Run the server with two task consumers (pulsar actors)

.. code::

    python manage.py -w 2

**4 - Queue tasks**

Launch a python shell and play with the api

.. code:: python

    >>> from manage import app
    >>> api = app().backend
    >>> task = api.queue_task('addition', a=4, b=6, wait=True)
    >>> task
    task.addition<i24ab99ddf2744902a375e039790dcbc4><SUCCESS>
    >>> task.result
    10
    >>> task.status_string
    'SUCCESS'
    
License
=============
This software is licensed under the BSD 3-clause License. See the LICENSE
file in the top distribution directory for the full license text.


.. _`google user group`: https://groups.google.com/forum/?fromgroups#!forum/python-pulsar
.. _`Luca Sbardella`: http://lucasbardella.com
.. _`Quantmind`: http://quantmind.com
