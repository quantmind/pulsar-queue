import time
import json
from datetime import datetime, timedelta
from asyncio import Future

from pulsar import (async, EventHandler, PulsarException, is_async,
                    ImproperlyConfigured, CANCELLED_ERRORS)
from pulsar.utils.string import gen_unique_id, to_string
from pulsar.utils.log import lazyproperty, LazyString

from .models import JobRegistry
from . import states


if hasattr(timedelta, "total_seconds"):
    timedelta_seconds = lambda delta: max(delta.total_seconds(), 0)

else:   # pragma    nocover
    def timedelta_seconds(delta):
        if delta.days < 0:
            return 0
        return delta.days * 86400 + delta.seconds + (delta.microseconds / 10e5)


def get_time(expiry, start):
    if isinstance(expiry, timedelta):
        return (start + 86400*expiry.days + expiry.seconds +
                0.000001*expiry.microseconds)
    else:
        return start + expiry


def format_time(dt):
    if isinstance(dt, (float, int)):
        dt = datetime.fromtimestamp(dt)
    return dt.isoformat() if dt else '?'


def nice_task_message(req, smart_time=None):
    smart_time = smart_time or format_time
    status = states.status_string(req.get('status'))
    user = req.get('user')
    ti = req.get('time_start', req.get('time_executed'))
    name = '%s (%s) ' % (req['name'], req['id'][:8])
    msg = '%s %s at %s' % (name, status, smart_time(ti))
    return '%s by %s' % (msg, user) if user else msg


class TaskNotAvailable(PulsarException):
    MESSAGE = 'Task {0} is not registered'

    def __init__(self, task_name):
        self.task_name = task_name
        super(TaskNotAvailable, self).__init__(self.MESSAGE.format(task_name))


class TaskTimeout(PulsarException):
    pass


class TaskConsumer(object):
    '''A context manager for consuming tasks.

    Instances of this consumer are created by the :class:`TaskBackend` when
    a task is executed.

    .. attribute:: task_id

        the :attr:`Task.id` being consumed.

    .. attribute:: job

        the :class:`.Job` which generated the task.

    .. attribute:: worker

        the :class:`.Actor` executing the task.

    .. attribute:: backend

        The :class:`.TaskBackend`. This is useful when creating
        tasks from within a :ref:`job callable <job-callable>`.
    '''
    def __init__(self, backend, worker, task, job):
        self.logger = worker.logger
        self.backend = backend
        self.worker = worker
        self.job = job
        self.task = task


class Task:
    '''A data :class:`.Model` containing task execution data.
    '''
    time_started = None
    time_ended = None
    result = None

    def __init__(self, id=None, name=None, time_queued=None,
                 expiry=None, status=None, kwargs=None, **kw):
        self.id = id
        self.name = name
        self.time_queued = time_queued
        self.expiry = expiry
        self.status = status
        self.kwargs = kwargs
        self.__dict__.update(kw)

    @classmethod
    def load(cls, data, method=None):
        method = method or 'json'
        if method == 'json':
            return cls(**json.loads(to_string(data)))
        else:
            raise ImproperlyConfigured('Unknown serialisation "%s"' % method)

    def serialise(self, method=None):
        method = method or 'json'
        if method == 'json':
            return json.dumps(self.__dict__)
        else:
            raise ImproperlyConfigured('Unknown serialisation "%s"' % method)

    def done(self):
        '''Return ``True`` if the :class:`Task` has finshed.

        Its status is one of :ref:`READY_STATES <task-ready-state>`.
        '''
        return self.get('status') in states.READY_STATES

    def status_string(self):
        '''A string representation of :attr:`status` code
        '''
        return states.status_string(self.get('status'))

    def info(self):
        return 'task.%s(%s)' % (self.name, self.id)

    def lazy_info(self):
        return LazyString(self.info)


class TaskBackend(EventHandler):
    '''A backend class for running :class:`.Task`.
    A :class:`TaskBackend` is responsible for creating tasks and put them
    into the distributed queue.
    It also schedules the run of periodic tasks if enabled to do so.

    .. attribute:: task_paths

        List of paths where to upload :ref:`jobs <app-taskqueue-job>` which
        are factory of tasks. Passed by the task-queue application
        :ref:`task paths setting <setting-task_paths>`.

    .. attribute:: schedule_periodic

        ``True`` if this :class:`TaskBackend` can schedule periodic tasks.

        Passed by the task-queue application
        :ref:`schedule-periodic setting <setting-schedule_periodic>`.

    .. attribute:: backlog

        The maximum number of concurrent tasks running on a task-queue
        for an :class:`.Actor`. A number in the order of 5 to 10 is normally
        used. Passed by the task-queue application
        :ref:`concurrent tasks setting <setting-concurrent_tasks>`.

    .. attribute:: max_tasks

        The maximum number of tasks a worker will process before restarting.
        Passed by the task-queue application
        :ref:`max requests setting <setting-max_requests>`.

    .. attribute:: poll_timeout

        The (asynchronous) timeout for polling tasks from the task queue.

        It is always a positive number and it can be specified via the
        backend connection string::

            local://?poll_timeout=3

        There shouldn't be any reason to modify the default value.

        Default: ``2``.

    .. attribute:: processed

        The number of tasks processed (so far) by the worker running this
        backend.
        This value is important in connection with the :attr:`max_tasks`
        attribute.

    '''
    task_poller = None

    def __init__(self, store,
                 logger=None,
                 task_paths=None,
                 schedule_periodic=False,
                 backlog=1,
                 max_tasks=0, name=None,
                 poll_timeout=None):
        super(TaskBackend, self).__init__(store._loop,
                                          many_times_events=('task_queued',
                                                             'task_started',
                                                             'task_done'))
        self.store = store
        self.name = name
        self.task_paths = task_paths
        self.backlog = backlog
        self.max_tasks = max_tasks
        self.poll_timeout = max(poll_timeout or 0, 2)
        self.concurrent_tasks = set()
        self.processed = 0
        self.schedule_periodic = schedule_periodic
        self.next_run = time.time()
        self._logger = logger
        self._callbacks = {}
        self._pubsub = self.get_pubsub()

    def __repr__(self):
        if self.schedule_periodic:
            return 'task scheduler %s' % self.store.dns
        else:
            return 'task consumer %s' % self.store.dns
    __str__ = __repr__

    @property
    def num_concurrent_tasks(self):
        '''The number of :attr:`concurrent_tasks`.

        This number is never greater than the :attr:`backlog` attribute.
        '''
        return len(self.concurrent_tasks)

    @lazyproperty
    def entries(self):
        return self._setup_schedule()

    @lazyproperty
    def registry(self):
        '''The :class:`.JobRegistry` for this backend.
        '''
        return JobRegistry.load(self.task_paths)

    def channel(self, name):
        '''Given an event ``name`` returns the corresponding channel name.

        The event ``name`` is one of ``task_queued``, ``task_started``
        or ``task_done``
        '''
        return '%s_%s' % (self.name, name)

    def event_name(self, channel):
        return channel[len(self.name)+1:]

    def queue_task(self, jobname, meta_params=None, expiry=None, **kwargs):
        '''Try to queue a new :ref:`Task`.

        This method returns a :class:`.Future` which results in the
        task ``id`` created. If ``jobname`` is not a valid
        :attr:`.Job.name`, a ``TaskNotAvailable`` exception occurs.

        :param jobname: the name of a :class:`.Job`
            registered with the :class:`.TaskQueue` application.
        :param meta_params: Additional parameters to be passed to the
            :class:`Task` constructor (not its callable function).
        :param expiry: optional expiry timestamp to override the default
            expiry of a task.
        :param kwargs: optional dictionary used for the key-valued arguments
            in the task callable.
        :return: a :class:`.Future` resulting in a task once finished
        '''
        if jobname in self.registry:
            job = self.registry[jobname]
            task_id = gen_unique_id()
            queued = time.time()
            if expiry is not None:
                expiry = get_time(expiry, queued)
            elif job.timeout:
                expiry = get_time(job.timeout, queued)
            meta_params = meta_params or {}
            task = Task(task_id,
                        name=job.name,
                        time_queued=queued,
                        expiry=expiry,
                        kwargs=kwargs,
                        status=states.QUEUED,
                        **meta_params)
            callback = Future(loop=self._loop)
            self._callbacks[task_id] = callback
            async(self._queue_task(task), loop=self._loop)
            return callback
        else:
            raise TaskNotAvailable(jobname)

    def get_pubsub(self):
        '''Create a publish/subscribe handler from the backend :attr:`store`.
        '''
        pubsub = self.store.pubsub()
        pubsub.add_client(self)
        # pubsub channels names from event names
        channels = tuple((self.channel(name) for name in self.events))
        pubsub.subscribe(*channels)
        self.bind_event('task_done', self._task_done_callback)
        return pubsub

    def flush(self):
        '''Remove all queued :class:`.Task`
        '''
        return self.store.flush()

    # #######################################################################
    # #    START/CLOSE METHODS FOR TASK WORKERS
    # #######################################################################
    def start(self, worker):
        '''Invoked by the task queue ``worker`` when it starts.
        '''
        self.may_pool_task(worker)
        self.logger.debug('started polling tasks')

    def close(self):
        '''Close this :class:`TaskBackend`.

        Invoked by the :class:`.Actor` when stopping.
        '''
        task = self.task_poller
        if task:
            task.cancel()
            self.task_poller = None
        self._pubsub.close()
        return task

    # #######################################################################
    # #    PRIVATE METHODS
    # #######################################################################
    def tick(self, now=None):
        # Run a tick, that is one iteration of the scheduler.
        if not self.schedule_periodic:
            return
        remaining_times = []
        for entry in self.entries.values():
            is_due, next_time_to_run = entry.is_due(now=now)
            if is_due:
                async(self.queue_task(entry.name))
            if next_time_to_run:
                remaining_times.append(next_time_to_run)
        self.next_run = now or time.time()
        if remaining_times:
            self.next_run += min(remaining_times)

    def job_list(self, jobnames=None):
        registry = self.registry
        jobnames = jobnames or registry
        all = []
        for name in jobnames:
            if name not in registry:
                continue
            job = registry[name]
            can_overlap = job.can_overlap
            if hasattr(can_overlap, '__call__'):
                can_overlap = 'maybe'
            d = {'doc': job.__doc__,
                 'doc_syntax': job.doc_syntax,
                 'type': job.type,
                 'can_overlap': can_overlap}
            if self.entries and name in self.entries:
                entry = self.entries[name]
                _, next_time_to_run = self.next_scheduled((name,))
                run_every = 86400*job.run_every.days + job.run_every.seconds
                d.update({'next_run': next_time_to_run,
                          'run_every': run_every,
                          'runs_count': entry.total_run_count})
            all.append((name, d))
        return all

    def next_scheduled(self, jobnames=None):
        if not self.schedule_periodic:
            return
        if jobnames:
            entries = (self.entries.get(name, None) for name in jobnames)
        else:
            entries = self.entries.values()
        next_entry = None
        next_time = None
        for entry in entries:
            if entry is None:
                continue
            is_due, next_time_to_run = entry.is_due()
            if is_due:
                next_time = 0
                next_entry = entry
                break
            elif next_time_to_run is not None:
                if next_time is None or next_time_to_run < next_time:
                    next_time = next_time_to_run
                    next_entry = entry
        if next_entry:
            return (next_entry.name, max(next_time, 0))
        else:
            return (jobnames, None)

    def may_pool_task(self, worker):
        assert self.task_poller is None
        if worker.is_running():
            self.task_poller = async(self._may_pool_task(worker))
        else:
            worker._loop.call_soon(self.may_pool_task, worker)

    # INTERNALS
    def _queue_task(self, task):
        '''Asynchronously wait for a task with ``task_id`` to have finished
        its execution.
        '''
        queue_name = self.channel('inqueue')
        stask = self._serialise(task)
        yield from self.store.client().lpush(queue_name, stask)
        yield from self._publish('queued', stask)
        scheduled = self.entries.get(task.name)
        if scheduled:
            scheduled.next()
        self.logger.debug('queued %s', task.lazy_info())

    def _may_pool_task(self, worker):
        # Called in the ``worker`` event loop.
        #
        # It pools a new task if possible, and add it to the queue of
        # tasks consumed by the ``worker`` CPU-bound thread.'''
        next_time = 0
        if worker.is_running():
            # executor = worker.executor()
            if self.num_concurrent_tasks < self.backlog:
                if self.max_tasks and self.processed >= self.max_tasks:
                    if not self.num_concurrent_tasks:
                        self.logger.warning('Processed %s tasks. Restarting.',
                                            self.processed)
                        worker._loop.stop()
                else:
                    try:
                        task = yield from self._get_task()
                    except ConnectionRefusedError:
                        if worker.is_running():
                            raise
                    except CANCELLED_ERRORS:
                        self.logger.debug('stopped polling tasks')
                        raise
                    if task:    # Got a new task
                        self.processed += 1
                        self.concurrent_tasks.add(task.id)
                        coro = self._execute_task(worker, task)
                        async(coro)
                        # executor.submit(yield_from, coro)
            else:
                self.logger.debug('%s concurrent requests. Cannot poll.',
                                  self.num_concurrent_tasks)
                next_time = 1
        self.task_poller = None
        worker._loop.call_later(next_time, self.may_pool_task, worker)

    def _get_task(self):
        '''Asynchronously retrieve a :class:`Task` from a ``task_id``.

        :param task_id: the ``id`` of the task to retrieve.
        :return: a :class:`Task` or ``None``.
        '''
        inq = self.channel('inqueue')
        client = self.store.client()
        stask = yield from client.execute('brpop', inq, self.poll_timeout)
        if stask:
            return Task.load(stask, self.cfg.params.get('TASK_SERIALISATION'))

    def _execute_task(self, worker, task):
        # Asynchronous execution of a Task. This method is called
        # on a separate thread of execution from the worker event loop thread.
        logger = worker.logger
        task_id = task.id
        time_ended = time.time()
        job = self.registry.get(task.name)
        consumer = TaskConsumer(self, worker, task, job)
        task_info = task.lazy_info()
        try:
            if not consumer.job:
                raise RuntimeError('%s not in registry' % task_info)
            if task.status > states.STARTED:
                expiry = task.expiry
                if expiry and time_ended > expiry:
                    raise TaskTimeout
                else:
                    logger.info('starting %s', task_info)
                    kwargs = task.kwargs or {}
                    task.status = states.STARTED
                    task.time_started = time_ended,
                    task.worker = worker.aid
                    yield from self._publish('started', task)
                    # This may block for a while
                    result = job(consumer, **kwargs)
                    if is_async(result):
                        task.result = yield from result
                    else:
                        task.result = result
                    task.status = states.SUCCESS
            else:
                logger.error('invalid status for %s', task_info)
        except TaskTimeout:
            logger.warning('%s timed-out', task_info)
            task.result = None
            task.status = states.REVOKED
        except Exception as exc:
            logger.exception('failure in %s', task_info)
            task.result = str(exc)
            task.status = states.FAILURE
        #
        task.time_ended = time.time()
        self.concurrent_tasks.discard(task_id)
        #
        logger.info('finished %s', task_info)
        yield from self._publish('done', task)

    def _serialise(self, task):
        return task.serialise(self.cfg.params.get('TASK_SERIALISATION'))

    def _setup_schedule(self):
        entries = {}
        if not self.schedule_periodic:
            return entries
        for name, t in self.registry.filter_types('periodic'):
            every = t.run_every
            if isinstance(every, int):
                every = timedelta(seconds=every)
            if not isinstance(every, timedelta):
                raise ValueError('Schedule %s is not a timedelta' % every)
            entries[name] = SchedulerEntry(name, every, t.anchor)
        return entries

    def _task_done_callback(self, task, exc=None):
        done = self.callbacks.pop(task.id, None)
        if done:
            done.set_result(task)
        else:
            self.logger.error('Could not find callback for %s',
                              task.lazy_info())

    def _publish(self, name, task):
        channel = self.channel('task_%s' % name)
        if isinstance(task, Task):
            task = self._serialise(task)
        return self._pubsub.publish(channel, task)

    def __call__(self, channel, message):
        # PubSub callback
        name = self.event_name(channel)
        self.fire_event(name, message)


class SchedulerEntry(object):
    '''A class used as a schedule entry by the :class:`.TaskBackend`.

    .. attribute:: name

        Task name

    .. attribute:: run_every

        Interval in seconds

    .. attribute:: anchor

        Datetime anchor

    .. attribute:: last_run_at

        last run datetime

    .. attribute:: total_run_count

        Total number of times this periodic task has been executed by the
        :class:`.TaskBackend`.
    '''
    def __init__(self, name, run_every, anchor=None):
        self.name = name
        self.run_every = run_every
        self.anchor = anchor
        self.last_run_at = datetime.now()
        self.total_run_count = 0

    def __repr__(self):
        return self.name
    __str__ = __repr__

    @property
    def scheduled_last_run_at(self):
        '''The scheduled last run datetime.

        This is different from :attr:`last_run_at` only when
        :attr:`anchor` is set.
        '''
        last_run_at = self.last_run_at
        anchor = self.anchor
        if last_run_at and anchor:
            run_every = self.run_every
            times = int(timedelta_seconds(last_run_at - anchor)
                        / timedelta_seconds(run_every))
            if times:
                anchor += times*run_every
                while anchor <= last_run_at:
                    anchor += run_every
                while anchor > last_run_at:
                    anchor -= run_every
                self.anchor = anchor
            return anchor
        else:
            return last_run_at

    def next(self, now=None):
        '''Increase the :attr:`total_run_count` attribute by one and set the
        value of :attr:`last_run_at` to ``now``.
        '''
        self.last_run_at = now or datetime.now()
        self.total_run_count += 1

    def is_due(self, now=None):
        '''Returns tuple of two items ``(is_due, next_time_to_run)``,
        where next time to run is in seconds.

        See :meth:`unuk.contrib.tasks.models.PeriodicTask.is_due`
        for more information.
        '''
        last_run_at = self.scheduled_last_run_at
        now = now or datetime.now()
        rem_delta = last_run_at + self.run_every - now
        rem = timedelta_seconds(rem_delta)
        if rem == 0:
            return True, timedelta_seconds(self.run_every)
        return False, rem
