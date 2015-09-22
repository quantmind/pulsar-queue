import asyncio


class PubSubImpl:
    '''Class implementing publish/subscribe for task producers

    It is needed for publishing task events to the broker as well
    as binding callbacks for inline task execution

    This class provide the pubsub implementation of :class:`.TaskProcucer`,
    none of its methods should be used by the API
    '''
    @classmethod
    def __new__(cls, *args, **kwargs):
        o = super().__new__(cls)
        o._callbacks = {}
        return o

    def __call__(self, channel, message):
        # PubSub callback
        name = channel[len(self._prefix()):]
        self.fire_event(name, message)

    def _get_pubsub(self):
        '''Create a publish/subscribe handler from the backend :attr:`store`.
        '''
        pubsub = self.store.pubsub()
        pubsub.add_client(self)
        pattern = '%s*' % self._prefix()
        asyncio.async(pubsub.psubscribe(pattern))
        self.bind_event('task_done', self._task_done_callback)
        return pubsub

    def _async_task(self, task):
        # Queue the task in the event loop and return a Future
        # called back once thetask is done
        callback = asyncio.Future(loop=self._loop)
        self._callbacks[task.id] = callback
        asyncio.async(self._queue_task(task), loop=self._loop)
        return callback

    def _publish(self, name, task):
        channel = task.channel('task_%s' % name)
        stask = self._serialise(task)
        return self._pubsub.publish(channel, stask)

    def _serialise(self, task):
        method = self.cfg.params.get('TASK_SERIALISATION')
        return task.serialise(method)

    def _task_done_callback(self, task, exc=None):
        done = self._callbacks.pop(task.id, None)
        if done:
            done.set_result(task)

    def _queue_task(self, task):
        '''Asynchronously queue a task
        '''
        stask = self._serialise(task)
        yield from self.store.client().lpush(task.queue, stask)
        yield from self._publish('queued', task)
        scheduled = self.entries.get(task.name)
        if scheduled:
            scheduled.next()
        self.logger.debug('queued %s in "%s"', task.lazy_info(), task.queue)

    def _prefix(self):
        return '%s_' % self.cfg.task_broadcast_prefix
