

class PubSubMixin:

    def _get_pubsub(self):
        '''Create a publish/subscribe handler from the backend :attr:`store`.
        '''
        pubsub = self.store.pubsub()
        pubsub.add_client(self)
        # pubsub channels names from event names
        # channels = tuple((self.channel(name) for name in self.events))
        # pubsub.subscribe(*channels)
        self.bind_event('task_done', self._task_done_callback)
        return pubsub

    def _publish(self, name, task):
        channel = task.channel('task_%s' % name)
        stask = self._serialise(task)
        return self._pubsub.publish(channel, stask)

    def _serialise(self, task):
        return task.serialise(self.cfg.params.get('TASK_SERIALISATION'))

    def __call__(self, channel, message):
        # PubSub callback
        name = self.event_name(channel)
        self.fire_event(name, message)
