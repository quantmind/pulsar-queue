|pulsar-queue|_

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


.. |pulsar-queue| image:: https://github.com/quantmind/pulsar-queue/blob/master/docs/pulsar-queuemonochrome.svg
   :width: 200 px
.. _master-build: https://github.com/quantmind/pulsar-queue/blob/master/docs/index.md
.. |master-build| image:: https://travis-ci.org/quantmind/pulsar-queue.svg?branch=master
.. _master-build: http://travis-ci.org/quantmind/pulsar-queue
.. |coverage-master| image:: https://coveralls.io/repos/github/quantmind/pulsar-queue/badge.svg?branch=master
  :target: https://coveralls.io/github/quantmind/pulsar-queue?branch=master



Three steps tutorial
------------------------

**Create a script which runs your application**:

.. code::

    vim manage.py


.. code:: python

    from pq import TaskApp


    task_paths = ['sampletasks.*']


    def app():
        return TaskApp(config=__file__)

    if __name__ == '__main__':
        app().start()


**Create the modules where Jobs are implemented**

It can be a directory containing several submodules.

.. code::


**3 - Run your script, sit back and relax**

Run your script with two task consumers (pulsar actors)

.. code::

    python manage.py -w 2

License
=============
This software is licensed under the BSD 3-clause License. See the LICENSE
file in the top distribution directory for the full license text.


.. _`google user group`: https://groups.google.com/forum/?fromgroups#!forum/python-pulsar
.. _`Luca Sbardella`: http://lucasbardella.com
.. _`Quantmind`: http://quantmind.com
