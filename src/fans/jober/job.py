"""
update job status by print:
https://stackoverflow.com/questions/5947742/how-to-change-the-output-color-of-echo-in-linux

redirect output of thread:
https://stackoverflow.com/questions/14890997/redirect-stdout-to-a-file-only-for-a-specific-thread
"""
import json
import uuid
import shlex
import shutil
import ctypes
import signal
import select
import asyncio
import tempfile
import threading
import subprocess
from typing import Iterable, Union

from fans.path import Path
from fans.datelib import native_now, from_native

from .run import JobRun


class Job:

    def __init__(self, spec: dict, context: dict = None):
        """
        spec: dict - specification of the job, of type {
                'name': str?, # name of the job
                'id': str?, # id of the job
                'cwd': str?, # current working directory
                'cmd': str?,
                'module': str?,
                'callable': (str|callable)?,
                'args': (str|tuple)?,
                'kwargs': (str|dict)?,
                'env': dict,
            }
            different type of job will use different spec.

            For command line job: {
                'cmd': str, # specify the command to execute,
            }
            For Python module job: {
                'module': Optional[str], # module name as in `python -m <module>`
                'args': Optional[str], # command line argument as in `python -m <module> <args>`
            }
            For Python callable job: {
                'callable': str|callable, # python callable or `<module>:<func>`
                'args': (str|tuple)?,
            }
        context: dict - providing things like: {
                'root_dir': str, # job's root dir path
            }
        """
        self.spec = spec

        self.name = spec.get('name')
        self.id = spec.get('id') or self.name or uuid.uuid4().hex
        self.cmd = spec.get('cmd')
        self.module = spec.get('module')
        self.callable = spec.get('callable')
        self.args = self.parse_args(spec.get('args'))
        self.kwargs = self.parse_kwargs(spec.get('kwargs'))
        self.cwd = spec.get('cwd')
        self.env = spec.get('env')

        context = context or {}
        self.context = context
        self.root_dir = self.ensure_root_dir(context.get('root_dir'))
        self.pubsub = context.get('pubsub')

        if self.cmd:
            self.type = 'executable'
        elif self.callable:
            self.type = 'callable'
        elif self.module:
            self.type = 'module'
        else:
            self.type = 'invalid'

        self.run_id_to_active_run = {}
        self.sched_job = None

    def info(self, latest_run: bool = False):
        ret = {
            **self.spec,
            'type': self.type,
            'name': self.name,
            'id': self.id,
            'cmd': self.cmd,
            'module': self.module,
            'callable': str(self.callable),
            'args': self.args,
            'kwargs': self.kwargs,
        }
        if latest_run:
            run = self.latest_run
            if run:
                ret.update({
                    'status': run.status,
                    'beg': run.beg,
                    'end': run.end,
                })
        return ret

    def __call__(self, *args, **kwargs):
        context = self.prepare_run()
        run = JobRun.make(self, args, kwargs, context)
        self.run_id_to_active_run[run.id] = run
        try:
            run()
        finally:
            del self.run_id_to_active_run[run.id]
        return run

    def prepare_run(self):
        limit_archived_runs = self.context.get('limit.archived.runs') or 0
        if limit_archived_runs:
            run_paths = list(self.root_dir.iterdir())
            if len(run_paths) >= limit_archived_runs:
                del_paths = sorted(run_paths)[0:len(run_paths) - limit_archived_runs + 1]
                for path in del_paths:
                    shutil.rmtree(path)

        run_id = make_run_id()
        run_dir = self.root_dir / run_id
        run_dir.ensure_dir()
        out_path = run_dir / 'out.log'
        return {
            'id': run_id,
            'root_dir': self.root_dir,
            'run_dir': run_dir,
            'out_path': out_path,
            'pubsub': self.pubsub,
        }

    @property
    def next_run(self):
        if not self.sched_job:
            return
        job = self.sched_job
        return from_native(job.trigger.get_next_fire_time(0, native_now())).datetime_str()

    @property
    def runs(self):
        for path in sorted(self.root_dir.iterdir(), reverse = True):
            run_id = path.name
            if run_id in self.run_id_to_active_run:
                yield self.run_id_to_active_run[run_id]
            else:
                yield JobRun.from_archived(path)

    @property
    def latest_run(self):
        return next(self.runs, None)

    @property
    def last_run(self):
        # TODO: test
        if not self.run_id_to_active_run:
            return None
        if len(self.run_id_to_active_run) == 1:
            return list(self.run_id_to_active_run.values())[0]
        else:
            return list(sorted(self.run_id_to_active_run.items()))[0]

    def parse_args(self, args):
        if args is None:
            return ()
        if isinstance(args, str):
            return shlex.split(args)
        if isinstance(args, [tuple, list]):
            return tuple(args)
        raise RuntimeError(f'invalid args {args}')

    def parse_kwargs(self, kwargs):
        if kwargs is None:
            return {}
        if isinstance(kwargs, str):
            return json.loads(kwargs)
        if isinstance(kwargs, dict):
            return kwargs
        raise RuntimeError(f'invalid kwargs {kwargs}')

    def ensure_root_dir(self, path: Union[str, Path]):
        if not path:
            path = Path(tempfile.gettempdir()) / self.id
        path = Path(path) / self.id
        path.ensure_dir()
        return path


def format_datetime_for_fname(dt):
    tz_str = dt.strftime('%z')[:5]
    tz_str = tz_str.replace('+', '_')
    tz_str = tz_str.replace('-', '__')
    return dt.strftime('%Y%m%d_%H%M%S_%f') + tz_str


def make_run_id():
    random_part = uuid.uuid4().hex[:8]
    return format_datetime_for_fname(native_now()) + '_' + random_part
