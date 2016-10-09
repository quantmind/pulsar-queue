import os
import json
import sys
import traceback
import asyncio
import logging
from asyncio import subprocess, streams


PQPATH = os.path.dirname(__file__)
PROCESS_FILE = os.path.join(PQPATH, "cpubound.py")
LOGGER = logging.getLogger('pulsar.queue.cpubound')


class Stream:
    '''Modify stream for remote logging
    '''
    def __init__(self, stream):
        self.stream = stream

    def __getattr__(self, name):
        return getattr(self.stream, name)

    def write(self, msg):
        if msg:
            msg = json.dumps(msg)
            self.stream.write('%d\n%s\n' % (len(msg), msg))


class RemoteLogger(logging.StreamHandler):
    terminator = ''

    def __init__(self):
        super().__init__(sys.stdout)

    def format(self, record):
        return {'levelno': record.levelno,
                'msg': super().format(record)}


class CpuTaskInfo:

    def __init__(self, job):
        self.job = job
        self.buffer = ''

    def feed(self, data):
        self.buffer += data.decode('utf-8')
        while self.buffer:
            p = self.buffer.find('\n')
            if p:
                length = int(self.buffer[:p])
                data = self.buffer[p+1:]
                if len(data) >= length + 1:
                    self.buffer = data[length+1:]
                    self.on_data(json.loads(data[:length]))
                else:
                    break
            else:
                break

    def on_data(self, data):
        if isinstance(data, dict):
            if 'cpubound_result' in data:
                self.job.task.result = data['cpubound_result']
            elif 'cpubound_failure' in data:
                data = data['cpubound_failure']
                self.job.task.result = data[0]
                self.job.task.stacktrace = data[1]
            elif 'levelno' in data:
                self.job.logger.log(data['levelno'], data['msg'])
        elif isinstance(data, str):
            data = data.rstrip()
            if data:
                print(data)


class StreamProtocol(subprocess.SubprocessStreamProtocol):

    def __init__(self, job):
        super().__init__(streams._DEFAULT_LIMIT, job._loop)
        self.info = CpuTaskInfo(job)
        self.error = CpuTaskInfo(job)

    def pipe_data_received(self, fd, data):
        if fd == 1:
            self.info.feed(data)
        elif fd == 2:
            self.error.feed(data)
        super().pipe_data_received(fd, data)


async def main(syspath, params, stask):
    logger = LOGGER
    try:
        sys.path[:] = json.loads(syspath)
        from pq.api import QueueApp

        params = json.loads(params)
        params.update({'python_path': False,
                       'parse_console': False})
        producer = await QueueApp(**params).api().start()
        task = producer.pubsub.decode(stask, 'json')
        #
        tasks = producer.tasks
        logger = tasks.logger
        JobClass = tasks.registry.get(task.name)
        if not JobClass:
            raise RuntimeError('%s not in registry' % task.name)
        job = JobClass(tasks, task)
        result = await job.green_pool.submit(job, **task.kwargs)
        sys.stdout.write({'cpubound_result': result})
    except Exception:
        exc_info = sys.exc_info()
        error = str(exc_info[1])
        stacktrace = traceback.format_tb(exc_info[2])
        sys.stderr.write({'cpubound_failure': (error, stacktrace)})
        msg = '%s\n%s' % (error, ''.join(stacktrace))
        logger.error(msg)


if __name__ == '__main__':
    sys.stdout = Stream(sys.stdout)
    sys.stderr = Stream(sys.stderr)
    logging.basicConfig(level=logging.DEBUG,
                        format='[pid=%(process)s] %(message)s',
                        handlers=[RemoteLogger()])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(*sys.argv[1:]))
