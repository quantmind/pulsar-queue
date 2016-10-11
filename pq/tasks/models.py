import sys
import logging
import inspect
import asyncio
from datetime import datetime, date

from pulsar.utils.slugify import slugify
from pulsar.utils.importer import import_modules
from pulsar.utils.log import lazyproperty
from pulsar.utils.string import to_bytes

from ..utils.concurrency import concurrency_name, ASYNC_IO


class ShellError(RuntimeError):

    @property
    def returncode(self):
        return self.args[1] if len(self.args) > 1 else 1


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
                 'concurrency': concurrency_name.get(job.concurrency),
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
        for mod in import_modules(paths, safe=True):
            for name, item in vars(mod).items():
                if name == JOB_LIST:
                    for job_cls in item:
                        self.register(job_cls)
                else:
                    self.register(item)
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
    abstract = True
    timeout = None
    expires = None
    max_retries = None
    retry_delay = 0
    max_concurrency = None
    doc_syntax = 'markdown'
    default_queue = None
    concurrency = None

    def __init__(self, tasks=None, task=None):
        self.backend = tasks.backend if tasks else None
        self.task = task
        if task:
            self.__dict__.update(task.meta)

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
    def wait(self):
        return self.backend.green_pool.wait

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
        return self.concurrency or ASYNC_IO

    def run_in_executor(self, callable, *args):
        '''Run a callable in the event loop executor.
        '''
        future = self._loop.run_in_executor(None, callable, *args)
        return self.green_pool.wait(future)

    def queue(self, jobname, meta_params=None, **kw):
        '''Queue a new task in the task queue
        '''
        assert self.backend, 'backend not available'
        meta_params = self._meta_params(meta_params)
        return self.backend.tasks.queue(jobname, meta_params=meta_params, **kw)

    def queue_local(self, jobname, meta_params=None, **kw):
        '''Queue a new task in the local task queue
        '''
        assert self.backend, 'backend not available'
        meta_params = self._meta_params(meta_params)
        return self.backend.tasks.queue_local(
            jobname, meta_params=meta_params, **kw)

    def _meta_params(self, meta_params=None):
        if meta_params is None:
            meta_params = {}
        meta_params['from_task'] = self.task.id
        return meta_params

    def lock(self, name=None, **kw):
        '''Acquire a lock if possible
        '''
        return self.backend.lock(name or self.name, **kw)

    async def shell(self, command, input=None, chdir=None, interactive=False,
                    interactive_stderr=None, stderr=None, stdout=None, **kw):
        """Execute a shell command
        :param command: command to execute
        :param input: optional input
        :param chdir: optional directory to execute the shell command from
        :param  interactive: display output as it becomes available
        :return: the output text
        """
        stdin = asyncio.subprocess.PIPE if input is not None else None
        if chdir:
            command = 'cd %s && %s' % (chdir, command)

        if interactive_stderr is None:
            interactive_stderr = interactive

        proc = await asyncio.create_subprocess_shell(
            command,
            stdin=stdin,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        if input is not None:
            proc._feed_stdin(to_bytes(input))

        msg, err = await asyncio.gather(
            _interact(proc, 1, interactive, stdout or sys.stdout),
            _interact(proc, 2, interactive_stderr, stderr or sys.stderr)
        )
        if proc.returncode:
            msg = err.decode('utf-8') or msg.decode('utf-8')
            raise ShellError(msg, proc.returncode)
        return msg.decode('utf-8')


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
        self.class_name = name
        self.attrs = attrs
        base = Job
        if run_every:
            self.attrs['run_every'] = run_every
            base = PeriodicJob
        self.bases = (base,)

    def __call__(self, callable):
        if not self.class_name:
            self.class_name = callable.__name__
        self.class_name = slugify(self.class_name, '_')
        self.attrs['__call__'] = callable
        self.attrs['__doc__'] = callable.__doc__
        cls = JobMetaClass(self.class_name, self.bases, self.attrs)
        module = inspect.getmodule(callable)
        job_list = getattr(module, JOB_LIST, None)
        if not job_list:
            job_list = []
            setattr(module, JOB_LIST, job_list)
        job_list.append(cls)
        return cls


# INTERNALS
async def _interact(proc, fd, interactive, out):
    transport = proc._transport.get_pipe_transport(fd)
    stream = proc.stdout if fd == 1 else proc.stderr
    output = b''
    while interactive:
        line = await stream.readline()
        if not line:
            break
        out.write(line.decode('utf-8'))
    else:
        output = await stream.read()
    transport.close()
    return output
