
# Tasks Concurrency

A task can run in one of four ``concurrency`` modes. If not specified, the default concurrency mode is [THREAD_IO](#thread_io). 

## ASYNC_IO

The asynchronous IO mode is associated with tasks which return
an asyncio [Future][] or a coroutine. These tasks run concurrently
in the worker event loop.

## GREEN_IO

The asynchronous IO mode is associated with tasks which return
an asyncio Future or a coroutine. These tasks run concurrently
in the worker event loop.

## THREAD_IO

The default concurrency model. It assumes the task performs blocking IO operations
which make it suitable to be run in the event loop [executor][].
You can use this model for most blocking operation unless
* Long running CPU bound
* Don't release the GIL


[Future]: https://docs.python.org/3/library/asyncio-task.html#asyncio.Future
[executor]: https://docs.python.org/3/library/asyncio-eventloop.html#executor
