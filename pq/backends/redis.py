from .. import mq


class MQ(mq.MQ):
    """Redis Message Broker
    """
    def __init__(self, backend, store):
        super().__init__(backend, store)
        self._client = store.client()

    async def get_message(self, *queues):
        '''Asynchronously retrieve a :class:`Task` from queues

        :return: a :class:`.Task` or ``None``.
        '''
        assert queues
        args = list(queues)
        args.append(self.cfg.task_pool_timeout)
        qt = await self._client.execute('brpop', *args)
        if qt:
            _, message = qt
            return self.decode(message)

    async def flush_queues(self, *queues):
        '''Clear a list of task queues
        '''
        pipe = self._client.pipeline()
        for queue in queues:
            pipe.execute('del', queue)
        await pipe.commit()

    async def queue_message(self, queue, message):
        '''Asynchronously queue a task
        '''
        await self._client.lpush(queue, message)

    async def size(self, *queues):
        pipe = self._client.pipeline()
        for queue in queues:
            pipe.execute('llen', queue)
        sizes = await pipe.commit()
        return sizes

    async def incr(self, name):
        concurrent = await self._client.incr(self.prefixed(name))
        return concurrent

    async def decr(self, name):
        concurrent = await self._client.decr(self.prefixed(name))
        return concurrent
