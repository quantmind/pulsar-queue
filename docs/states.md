# Task states

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
