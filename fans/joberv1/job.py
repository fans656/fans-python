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
import pathlib
import tempfile
import traceback
from typing import Iterable, Union, Callable, List

from fans.fn import noop, parse_int
from fans.path import Path
from fans.logger import get_logger
from fans.datelib import native_now, from_native, Timestamp

from .run import Run, DummyRun


logger = get_logger(__name__)


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
            retry: any = None,
            config: dict = None,
            on_event: Callable[[dict], None] = None,
            on_retry: Callable[['Job'], None] = None,
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
        self.retry = self.parse_retry(retry)
        self.config = config or {}
        self.root_dir = self.ensure_root_dir(self.config.get('runs_dir'))
        self.on_event = on_event or noop
        self.on_retry = on_retry or noop

        if self.cmd:
            self.type = 'command'
        elif self.script:
            self.type = 'script'
            self.script, args = split_trailing_args(self.script)
            self.args = self.args or args
        elif self.module:
            self.type = 'module'
            self.module, args = split_trailing_args(self.module)
            self.args = self.args or args
        else:
            self.type = 'invalid'

        self.run_id_to_active_run = {}
        self.sched_job_id_to_sched_job = {}

    def __call__(self, context = None):
        run = self.prepare_run(context = context)
        self.run_id_to_active_run[run.id] = run
        try:
            run()
        except:
            traceback.print_exc()
        finally:
            del self.run_id_to_active_run[run.id]
        return run

    def kill(self):
        run = self.latest_run
        return self.latest_run.kill()

    def terminate(self):
        run = self.latest_run
        return self.latest_run.terminate()

    def process_event(self, event):
        event.update({
            'job_id': self.id,
            'next_run_time': self.next_run_time,
        })
        self.on_event(event)

    def process_exit(self, run):
        if run.returncode != 0:
            self.process_retry(run)

    def process_retry(self, run):
        if not self.retry:
            return
        if not run.context or run.context.get('remaining_retry', 0) > 0:
            self.on_retry(self, run)

    def info(self):
        ret = {
            'type': self.type,
            'name': self.name,
            'id': self.id,
            'cmd': self.cmd,
            'module': self.module,
            'args': self.args,
            'status': self.status,
            'error': self.error,
            'beg': Timestamp.to_datetime_str(self.beg),
            'end': Timestamp.to_datetime_str(self.end),
            'next_run_time': self.next_run_time,
            'sched': self.sched,
            'retry': self.retry,
            'config': self.config,
        }
        return ret

    def get_run_by_id(self, id):
        return next((run for run in self.runs if run.id == id), None)

    @property
    def encoding(self):
        return self.config.get('encoding') or 'utf-8'

    @property
    def status(self):
        return self.latest_run.status

    @property
    def error(self):
        return self.latest_run.error

    @property
    def beg(self):
        return self.latest_run.beg

    @property
    def end(self):
        return self.latest_run.end

    @property
    def next_run_time(self) -> str:
        tss = list(filter(bool, [
            sched_job.trigger.get_next_fire_time(None, native_now())
            for sched_job in self.sched_job_id_to_sched_job.values()
        ]))
        if tss:
            return min(from_native(ts).datetime_str() for ts in tss)
        else:
            return None

    @property
    def latest_run(self):
        return next(self.runs, dummy_run)

    @property
    def runs(self):
        for path in sorted(self.root_dir.iterdir(), reverse = True):
            run_id = path.name
            if run_id in self.run_id_to_active_run:
                # active run, attrs (e.g. status) will change over time
                yield self.run_id_to_active_run[run_id]
            else:
                # archived run, attrs already fixed
                yield Run.from_archived(path)

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

    def parse_retry(self, retry):
        if retry is None:
            return {
                'times': 0,
            }
        elif isinstance(retry, bool):
            return {
                'times': 1,
                'delay': 300,
            }
        elif isinstance(retry, dict):
            return {
                'times': parse_int(retry.get('times'), 1),
                'delay': parse_int(retry.get('delay'), 300),
            }
        else:
            raise RuntimeError(f'unsupported retry spec: {retry}')

    def prepare_run(self, context = None):
        self.clear_old_runs()
        return self.make_run(context = context)

    def clear_old_runs(self):
        value = self.config.get('limit.archived.runs') or 0
        try:
            limit = int(value)
        except:
            logger.warning(f'invalid value for limit.archived.runs: {value}')
            return
        if limit > 0:
            run_paths = list(self.root_dir.iterdir())
            if len(run_paths) >= limit:
                for path in sorted(run_paths)[0:len(run_paths) - limit + 1]:
                    shutil.rmtree(path)

    def make_run(self, context = None):
        run_id = make_run_id()
        run_dir = self.root_dir / run_id
        run_dir.ensure_dir()
        return Run(
            {
                'type': self.type,
                'cmd': self.cmd,
                'script': self.script,
                'module': self.module,
                'args': self.args,
                'cwd': self.cwd,
                'env': self.env,
            },
            run_dir,
            id = run_id,
            on_event = self.process_event,
            on_exit = self.process_exit,
            context = context,
        )

    def ensure_root_dir(self, path: Union[str, Path]):
        if not path:
            path = Path(tempfile.gettempdir()) / 'fans.jober'
        path = Path(path) / self.id
        path.ensure_dir()
        return path

    def with_retry(self):
        func = lambda: self(retrying = 1) # TODO: only support 1 times for now
        return func, self

    def __repr__(self):
        return f'Job(name = {self.name})'


def split_trailing_args(value):
    parts = shlex.split(value)
    if len(parts) == 1:
        return parts[0], []
    elif len(parts) > 1:
        return parts[0], parts[1:]
    else:
        raise RuntimeError('empty shlex string')


def format_datetime_for_fname(dt):
    tz_str = dt.strftime('%z')[:5]
    tz_str = tz_str.replace('+', '_')
    tz_str = tz_str.replace('-', '__')
    return dt.strftime('%Y%m%d_%H%M%S_%f') + tz_str


def make_run_id():
    random_part = uuid.uuid4().hex[:8]
    return format_datetime_for_fname(native_now()) + '_' + random_part


dummy_run = DummyRun()
