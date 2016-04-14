'''
A :ref:`task queue <apps-taskqueue>` application implements several
:class:`Job` classes which specify the way a :class:`.Task` is run.
Each :class:`Job` class is a :class:`.Task` factory, therefore,
a :class:`.Task` is always associated
with one :class:`Job`, which can be of two types:

* standard (:class:`.Job`)
* periodic (:class:`.PeriodicJob`), a generator of scheduled tasks.

.. _job-callable:

Job callable method
~~~~~~~~~~~~~~~~~~~~~~~~~~

To define a job is simple, subclass from :class:`.Job` and implement the
**job callable method**::

    from pulsar.apps import tasks

    class Addition(tasks.Job):

        def __call__(self, consumer, a=0, b=0):
            "Add two numbers"
            return a+b

The ``consumer``, instance of :class:`.TaskConsumer`,
is passed by the :ref:`Task backend <apps-taskqueue-backend>` and should
always be the first positional parameter in the callable method.
The remaining (optional key-valued only!) parameters are needed by
your job implementation.

A :ref:`job callable <job-callable>` can also return a
:ref:`coroutine <coroutine>` if it needs to perform asynchronous IO during its
execution::

    class Crawler(tasks.Job):

        def __call__(self, consumer, sample=100, size=10):
            response = yield http.request(...)
            content = response.content
            ...

This allows for cooperative task execution.

.. _job-non-overlap:

Non overlapping Jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~

The :attr:`~.Job.can_overlap` attribute controls the way tasks are generated
by a specific :class:`.Job`. By default, a :class:`.Job` creates a new task
every time the :class:`.TaskBackend` requests it.

However, when setting the :attr:`~.Job.can_overlap` attribute to ``False``,
a new task cannot be started unless a previous task of the same job
is done.

'''
import logging
import inspect
from datetime import datetime, date

from pulsar.utils.slugify import slugify
from pulsar.utils.importer import import_modules
from pulsar.utils.log import lazyproperty


ASYNC_IO = 1        # tasks run in the worker event loop
GREEN_IO = 2        # tasks run in the worker event loop on a child greenlet
THREAD_IO = 3       # tasks run in the event loop executor
CPUBOUND = 4        # tasks run in a subprocess


_concurrency = {'asyncio': ASYNC_IO,
                'greenio': GREEN_IO,
                'thread': THREAD_IO,
                'process': CPUBOUND}


class RegistryMixin:

    @lazyproperty
    def registry(self):
        '''The :class:`.JobRegistry` for this backend.
        '''
        return JobRegistry.load(self.cfg.task_paths)

    def job_list(self, jobnames=None):
        registry = self.registry
        jobnames = jobnames or registry
        all = []
        for name in jobnames:
            if name not in registry:
                continue
            job = registry[name]()
            d = {'doc': job.__doc__,
                 'doc_syntax': job.doc_syntax,
                 'type': job.type}
            all.append((name, d))
        return all


class JobRegistry(dict):
    """Site registry for tasks."""

    def regular(self):
        """A tuple containing of all regular jobs."""
        return tuple(self.filter_types(type="regular"))

    def periodic(self):
        """A tuple containing all periodic jobs."""
        return tuple(self.filter_types(type="periodic"))

    def register(self, job):
        """Register a job in the job registry.

        The task will be automatically instantiated if not already an
        instance.

        """
        if isinstance(job, JobMetaClass) and job.can_register:
            name = job.name
            self[name] = job

    def filter_types(self, type=None, queue=None):
        """Return a generator of all tasks of a specific type."""
        for name, jobClass in self.items():
            job = jobClass()
            if type and job.type == type:
                yield name, jobClass

    @classmethod
    def load(cls, paths):
        self = cls()
        for mod in import_modules(paths, safe=False):
            for name in dir(mod):
                if name == JOB_LIST:
                    for job_cls in getattr(mod, name):
                        self.register(job_cls)
                else:
                    self.register(getattr(mod, name))
        return self


class JobMetaClass(type):

    def __new__(cls, name, bases, attrs):
        attrs['can_register'] = not attrs.pop('abstract', False)
        job_name = slugify(attrs.get("name", name), '.')
        log_prefix = attrs.get("log_prefix") or "pulsar.queue"
        attrs["name"] = job_name
        logname = '%s.%s' % (log_prefix, job_name)
        attrs['logger'] = logging.getLogger(logname)
        return super(JobMetaClass, cls).__new__(cls, name, bases, attrs)


class Job(metaclass=JobMetaClass):
    '''The Job class which is used in a distributed task queue.

    .. attribute:: name

        The unique name which defines the Job and which can be used to retrieve
        it from the job registry. This attribute is set to the Job class name
        in lower case by default, unless a ``name`` class attribute is defined.

    .. attribute:: abstract

        If set to ``True`` (default is ``False``), the :class:`.Job` won't be
        registered with the :class:`.JobRegistry`. Useful when creating a new
        base class for several other jobs.

    .. attribute:: type

        Type of Job, one of ``regular`` and ``periodic``.

    .. attribute:: concurrency

        Concurrency type can be either: ASYNC_IO, GREEN_IO, THREAD_IO
        or CPUBOUND.
        Tasks of the first two concurrency types are run concurrently in
        the event loop of workers, THREAD_IO tasks run in the evnt loop
        executor while the last one is associated with CPU-bound tasks and
        a run in subprocess.

        Default: ``CPUBOUND``

    .. attribute:: timeout

        An instance of a datetime.timedelta or ``None``.
        If set, it represents the time lag after which a task which
        did not start expires.

        Default: ``None``.

    .. attribute:: doc_syntax

        The doc string syntax.

        Default: ``markdown``

    .. attribute:: logger

        an instance of a logger. Created at runtime.
    '''
    abstract = True
    timeout = None
    expires = None
    doc_syntax = 'markdown'
    can_overlap = True
    queue = None
    concurrency = None

    def __init__(self, backend=None, task=None):
        self.backend = backend
        self.task = task

    def __repr__(self):
        return 'job.%s' % self.task if self.task else self.name
    __str__ = __repr__

    def __call__(self, **kwargs):
        raise NotImplementedError("Jobs must implement the __call__ method.")

    @property
    def cfg(self):
        """Configuration  object from :attr:`backend`"""
        return self.backend.cfg

    @property
    def green_pool(self):
        return self.backend.green_pool

    @property
    def http(self):
        """Best possible HTTP session handler
        """
        return self.backend.http_sessions(self.get_concurrency())

    @property
    def _loop(self):
        return self.backend._loop if self.backend else None

    @property
    def type(self):
        '''Type of Job, one of ``regular`` and ``periodic``.'''
        return 'regular'

    def get_concurrency(self):
        '''The concurrency for this job
        '''
        default = self.backend.cfg.default_task_concurrency
        return self.concurrency or _concurrency[default.lower()]

    def run_in_executor(self, callable, *args, **kw):
        '''Run a callable in the event loop executor.

        If the concurrency is set to GRENN_IO, wait for the future
        to execute
        '''
        future = self._loop.run_in_executor(None, callable, *args)
        concurrency = self.get_concurrency()
        if concurrency == GREEN_IO:
            return self.green_pool.wait(future)
        else:
            return future

    def queue_task(self, jobname, meta_params=None, **kw):
        '''Queue a new task in the task queue
        '''
        assert self.backend, 'backend not available'
        meta_params = self._meta_params(meta_params)
        return self.backend.queue_task(jobname, meta_params=meta_params, **kw)

    def queue_task_local(self, jobname, meta_params=None, **kw):
        '''Queue a new task in the local task queue
        '''
        assert self.backend, 'backend not available'
        meta_params = self._meta_params(meta_params)
        return self.backend.queue_task_local(
            jobname, meta_params=meta_params, **kw)

    def _meta_params(self, meta_params=None):
        if meta_params is None:
            meta_params = {}
        meta_params['from_task'] = self.task.id
        return meta_params

    def lock(self, name=None, revoke=False):
        '''Acquire a lock if possible
        '''
        name = name or self.name
        return self.backend.lock(name, revoke)


class PeriodicJob(Job):
    '''A periodic :class:`.Job` implementation.'''
    abstract = True
    anchor = None
    '''If specified it must be a :class:`~datetime.datetime` instance.
    It controls when the periodic Job is run.
    '''
    run_every = None
    '''Periodicity as a :class:`~datetime.timedelta` instance.'''

    @property
    def type(self):
        return 'periodic'

    def is_due(self, last_run_at):
        """Returns tuple of two items ``(is_due, next_time_to_run)``,
        where next time to run is in seconds. For example:

        * ``(True, 20)``, means the job should be run now, and the next
          time to run is in 20 seconds.

        * ``(False, 12)``, means the job should be run in 12 seconds.

        You can override this to decide the interval at runtime.
        """
        return self.run_every.is_due(last_run_at)


class EventDriven(Job):

    @property
    def type(self):
        return 'event-driven'


def anchorDate(hour=0, minute=0, second=0):
    '''Create an anchor date.'''
    td = date.today()
    return datetime(year=td.year, month=td.month, day=td.day,
                    hour=hour, minute=minute, second=second)


JOB_LIST = '__PULSAR_QUEUE_JOBS__'


class job:
    """Decorator for creating a Job class from a function
    """
    def __init__(self, name=None, run_every=None, **attrs):
        assert name, 'task requires a valid name'
        self.class_name = slugify(name, '_')
        self.attrs = attrs
        base = Job
        if run_every:
            self.attrs['run_every'] = run_every
            base = PeriodicJob
        self.bases = (base,)

    def __call__(self, callable):
        self.attrs['__call__'] = callable
        cls = JobMetaClass(self.class_name, self.bases, self.attrs)
        module = inspect.getmodule(callable)
        job_list = getattr(module, JOB_LIST, None)
        if not job_list:
            job_list = []
            setattr(module, JOB_LIST, job_list)
        job_list.append(cls)
        return cls
