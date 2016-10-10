from pulsar.utils.structures import inverse_mapping


ASYNC_IO = 1        # run in the worker event loop
THREAD_IO = 3       # run in the event loop executor
CPUBOUND = 4        # run in a subprocess


concurrency = {'asyncio': ASYNC_IO,
               'thread': THREAD_IO,
               'process': CPUBOUND}

concurrency_name = dict(inverse_mapping(concurrency))
