
# Tasks Concurrency

A task can run in one of four ``concurrency`` modes.
If not specified by the [Job][], the concurrency mode is given by the
[default_task_concurrency][] parameter.

## ASYNC_IO

The asynchronous IO mode is associated with tasks which return
an asyncio [Future][] or a coroutine. These tasks run concurrently
in the worker event loop.

## GREEN_IO

The asynchronous IO mode is associated with tasks which return
an asyncio Future or a coroutine. These tasks run concurrently
in the worker event loop.

## THREAD_IO

It assumes the task performs blocking IO operations
which make it suitable to be run in the event loop [executor][].
You can use this model for most blocking operation unless

* Long running CPU bound
* The operation does not release the GIL

## CPUBOUND

It assumes the task performs blocking CPUT bound operations.
These tasks are run on sub-processes.

[Future]: https://docs.python.org/3/library/asyncio-task.html#asyncio.Future
[executor]: https://docs.python.org/3/library/asyncio-eventloop.html#executor
[default_task_concurrency]: ./config.html#default-task-concurrency
