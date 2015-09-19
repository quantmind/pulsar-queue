=================
Pulsar Queue
=================

To get started, follow these guidelines:

* Create a script which runs your application
* Create the modules where Jobs are implemented. It
  can be a directory containing several submodules.
* Run your script, sit back and relax.

Configuration
=================

A :class:`TaskQueue` accepts several configuration parameters on top of the
standard :ref:`application settings <settings>`:

.. _app-tasks_path:

* The :ref:`task_paths <setting-task_paths>` parameter specifies
  a list of python paths where to collect :class:`.Job` classes::

      task_paths = ['myjobs','another.moduledir.*']

  The ``*`` at the end of the second module indicates to collect
  :class:`.Job` from all submodules of ``another.moduledir``.

* The :ref:`schedule_periodic <setting-schedule_periodic>` flag indicates
  if the :class:`TaskQueue` can schedule :class:`.PeriodicJob`. Usually,
  only one running :class:`TaskQueue` application is responsible for
  scheduling tasks.

  It can be specified in the command line via the
  ``--schedule-periodic`` flag.

  Default: ``False``.

* The :ref:`concurrent_tasks <setting-concurrent_tasks>` parameter controls
  the maximum number of concurrent tasks for a given task worker.
  This parameter is important when tasks are asynchronous, that is when
  they perform some sort of I/O and the :ref:`job callable <job-callable>`
  returns and :ref:`asynchronous component <tutorials-coroutine>`.

  It can be specified in the command line via the
  ``--concurrent-tasks ...`` option.

  Default: ``5``.

.. _celery: http://celeryproject.org/

The :class:`TaskBackend` is at the heart of the
:ref:`task queue application <apps-taskqueue>`. It exposes
all the functionalities for running new tasks, scheduling periodic tasks
and retrieving task information. Pulsar ships with two backends, one which uses
pulsar internals and store tasks in the arbiter domain and another which stores
tasks in redis_.

The backend is created by the :class:`.TaskQueue`
as soon as it starts. It is then passed to all task queue workers
which, in turns, invoke the :class:`TaskBackend.start` method
to start pulling tasks form the distributed task queue.

.. _task-state:

Task states
~~~~~~~~~~~~~

A :class:`Task` can have one of the following :attr:`~.Task.status` string:

* ``QUEUED = 6`` A task queued but not yet executed.
* ``STARTED = 5`` task where execution has started.
* ``RETRY = 4`` A task is retrying calculation.
* ``REVOKED = 3`` the task execution has been revoked (or timed-out).
* ``FAILURE = 2`` task execution has finished with failure.
* ``SUCCESS = 1`` task execution has finished with success.

.. _task-run-state:

**FULL_RUN_STATES**

The set of states for which a :class:`Task` has run:
``FAILURE`` and ``SUCCESS``

.. _task-ready-state:

**READY_STATES**

The set of states for which a :class:`Task` has finished:
``REVOKED``, ``FAILURE`` and ``SUCCESS``

.. _tasks-pubsub:

Task status broadcasting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A :class:`TaskBackend` broadcast :class:`Task` state into three different
channels via the a :meth:`~.Store.pubsub` handler.
