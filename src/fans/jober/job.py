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
import pathlib
import asyncio
import tempfile
import traceback
import threading
import subprocess
from typing import Iterable, Union, Callable, List

from fans.fn import noop
from fans.path import Path
from fans.datelib import native_now, from_native

from .run import Run


class Job:

    def __init__(
            self,
            name: str = None,
            id: str = None,
            cmd: str = None,
            script: str = None,
            module: str = None,
            args: Union[str, List[str]] = None,
            cwd: str = None,
            env: Union[str, dict] = None,
            sched: any = None,
            # root directory to store job related data, each job will have separate directory
            # under this root, e.g. <root_dir>/quantix.pricer/
            root_dir: Union[str, pathlib.Path] = None,
            on_event: Callable[[dict], None] = None,
            context: dict = None,
    ):
        self.name = name
        self.id = id or name or uuid.uuid4().hex
        self.cmd = cmd
        self.script = script
        self.module = module
        self.args = self.parse_args(args)
        self.cwd = cwd
        self.env = self.parse_dict(env)
        self.sched = sched

        context = context or {}
        self.context = context
        self.root_dir = self.ensure_root_dir(root_dir)
        self.on_event = on_event or noop

        if self.cmd:
            self.type = 'command'
        elif self.script:
            self.type = 'script'
        elif self.module:
            self.type = 'module'
            if ' ' in self.module:
                parts = self.module.split()
                self.module = parts[0]
                self.args = parts[1:]
        else:
            self.type = 'invalid'

        self.run_id_to_active_run = {}
        self.sched_job = None

    def info(self, latest_run: bool = False):
        ret = {
            'type': self.type,
            'name': self.name,
            'id': self.id,
            'cmd': self.cmd,
            'module': self.module,
            'args': self.args,
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

    def __call__(self):
        run = self.make_run()
        self.run_id_to_active_run[run.id] = run
        try:
            run()
        except:
            traceback.print_exc()
        finally:
            del self.run_id_to_active_run[run.id]
        return run

    def make_run(self):
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
        return Run(
            run_spec = {
                'type': self.type,
                'cmd': self.cmd,
                'script': self.script,
                'module': self.module,
                'args': self.args,
                'cwd': self.cwd,
                'env': self.env,
            },
            id = run_id,
            run_dir = run_dir,
            on_event = self.process_event,
        )

    def process_event(self, event):
        event.update({
            'job': self.id,
            'next_run': self.next_run,
        })
        self.on_event(event)

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
                yield Run.from_archived(path)

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

    def parse_dict(self, value, hint = None):
        if value is None:
            return {}
        if isinstance(value, str):
            return json.loads(value)
        if isinstance(value, dict):
            return value
        raise RuntimeError(f'invalid {hint or "value"} {value}')

    def ensure_root_dir(self, path: Union[str, Path]):
        if not path:
            path = Path(tempfile.gettempdir()) / 'fans.jober'
        path = Path(path) / self.id
        path.ensure_dir()
        return path

    def __repr__(self):
        return f'Job<{self.name or self.id}>'


def format_datetime_for_fname(dt):
    tz_str = dt.strftime('%z')[:5]
    tz_str = tz_str.replace('+', '_')
    tz_str = tz_str.replace('-', '__')
    return dt.strftime('%Y%m%d_%H%M%S_%f') + tz_str


def make_run_id():
    random_part = uuid.uuid4().hex[:8]
    return format_datetime_for_fname(native_now()) + '_' + random_part
