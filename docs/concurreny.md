
# Tasks Concurrency

A task can run in one of four concurrency modes:

## ASYNC_IO

The asynchronous IO mode is associated with tasks which return
an asyncio Future or a coroutine. These tasks run concurrently
in the worker event loop.

## GREEN_IO

The asynchronous IO mode is associated with tasks which return
an asyncio Future or a coroutine. These tasks run concurrently
in the worker event loop.
