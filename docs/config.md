[Index](./index.md) > Configuration
***

# Configuration

A `TaskApp` accepts several configuration parameters on top of the
standard application settings in pulsar:

## task_paths

This parameter specifies a list of python paths where to collect
`Job` classes:
```
      task_paths = ['myjobs','another.moduledir.*']
```
  The ``*`` at the end of the second module indicates to collect
  :class:`.Job` from all submodules of ``another.moduledir``.

## schedule_periodic

When ``True``, the task application can schedule [PeriodicJob][]. Usually,
only one running :class:`TaskQueue` application is responsible for
scheduling tasks.
This parameter can be set at the command line via the ``--schedule-periodic``
flag.

**Default**: ``False``.

## concurrent_tasks

Default maximu number of concurrent tasks for a given task consumer.

It can be specified in the command line via the
``--concurrent-tasks ...`` option.

**Default**: ``5``.
