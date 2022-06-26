import json
import uuid
import shlex
import ctypes
import signal
import select
import asyncio
import tempfile
import threading
import subprocess
from typing import Iterable, Union

from fans.path import Path
from fans.datelib import native_now, now


class JobRun:

    @classmethod
    def make(cls, job, args, kwargs, context):
        return cls({
            'id': context['id'],
            'job': job,
            'args': args,
            'kwargs': kwargs,
            'run_dir': context['run_dir'],
            'out_path': context['out_path'],
            'pubsub': context.get('pubsub'),
        })

    @classmethod
    def from_archived(cls, path):
        return cls((path / 'meta.json').load())

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
        if self.pubsub:
            event = {
                'event': 'job_run_status_changed',
                'id': self.id,
                'status': self._status,
                'job': self.job.id,
                'next_run': self.job.next_run,
            }
            self.pubsub.publish(event)

    def __init__(self, spec):
        self.id = spec.get('id')
        self.args = spec.get('args')
        self.kwargs = spec.get('kwargs')

        self.beg = spec.get('beg')
        self.end = spec.get('end')

        self.job = spec.get('job')
        self.run_dir = spec.get('run_dir')
        self.out_path = spec.get('out_path')
        self.out_file = None

        self._status = 'init'
        self.pubsub = spec.get('pubsub')
        self.finished = False

    def info(self):
        return {
            'id': self.id,
            'beg': self.beg,
            'end': self.end,
        }

    @property
    def output(self) -> str:
        with self.out_path.open() as f:
            return f.read()

    def iter_output(self) -> Iterable[str]:
        with self.out_path.open() as f:
            while True:
                for line in iter(f.readline, ''):
                    yield line[:-1]
                _, _, error = select.select([f], [], [f], 0.01)
                if error or self.finished:
                    break

    async def iter_output_async(self, loop: 'asyncio.base_events.BaseEventLoop' = None):
        def collect():
            with self.out_path.open() as f:
                while True:
                    for line in iter(f.readline, ''):
                        loop.call_soon_threadsafe(que.put_nowait, line)
                    _, _, error = select.select([f], [], [f], 0.01)
                    if error or self.finished:
                        break
        loop = loop or asyncio.get_event_loop()
        que = asyncio.Queue()
        thread = threading.Thread(target = collect)
        thread.start()
        while line := await que.get():
            yield line[:-1]

    def __call__(self, *args, **kwargs):
        self.beg = now()
        try:
            if self.out_path:
                self.out_file = self.out_path.open('w+', buffering = 1)
            self.status = 'running'
            if self.job:
                if self.job.type == 'executable':
                    self.run_executable()
        except:
            self.status = 'error'
        finally:
            self.status = 'done'
            if self.out_file:
                self.out_file.close()
            self.end = now()

            if self.run_dir:
                meta_path = Path(self.run_dir) / 'meta.json'
                meta_path.save({
                    'id': self.id,
                    'args': self.args,
                    'kwargs': self.kwargs,
                    'beg': self.beg.datetime_str(),
                    'end': self.end.datetime_str() if self.end else None,
                }, indent = 2)

    def run_executable(self):
        proc = subprocess.Popen(
            self.job.cmd,
            stdout = subprocess.PIPE,
            stderr = subprocess.STDOUT, # redirect to stdout
            bufsize = 1,
            encoding = 'utf-8',
            universal_newlines = True,
            shell = True,
            cwd = self.job.cwd,
            env = self.job.env,
            preexec_fn = exit_on_parent_exit,
        )
        try:
            for line in iter(proc.stdout.readline, ''):
                self.out_file.write(line)
        except KeyboardInterrupt:
            pass
        finally:
            proc.wait()
            self.finished = True


def exit_on_parent_exit():
    try:
        ctypes.cdll['libc.so.6'].prctl(1, signal.SIGHUP)
    except:
        pass
