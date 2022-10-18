import os
import sys
import uuid
import select
import signal
import asyncio
import pathlib
import tempfile
import threading
import traceback
import subprocess
from typing import Iterable, Union, Callable, List

from fans.fn import noop
from fans.path import Path
from fans.datelib import now, Timestamp


class Run:
    """
    Represent a single execution of runnable.
    """

    @classmethod
    def from_archived(cls, path: Path):
        return cls(
            **(path / 'meta.json').load(),
            run_dir = path,
        )

    def __init__(
            self,
            run_spec: dict,
            run_dir: Union[str, pathlib.Path] = None,
            id: str = None,
            beg: Union[str, Timestamp] = None,
            end: Union[str, Timestamp] = None,
            status: str = None,
            error: str = None,
            context: dict = None,
            on_event: Callable[[dict], None] = None,
            on_exit: Callable[['Run'], None] = None,
            **__,
    ):
        """
        Args:
            run_spec: dict - spec of the run, samples:

                {
                    'cmd': 'for i in 1 2 3; do echo $i; sleep 1; done',
                }

                {
                    'script': '/home/fans656/t.py',
                    'args': '--help',
                }

                {
                    'module': 'quantix.pricer.main -u',
                    'cwd': '/home/fans656/quantix',
                }

            id: str - ID of the run, if not given, will generate a new UUID.
            ...
        """
        self.run_spec = run_spec
        self.run_dir = Path(run_dir) if run_dir else None
        self.id = id or uuid.uuid4().hex
        self.beg = Timestamp.from_datetime_str(beg)
        self.end = Timestamp.from_datetime_str(end)
        self.context = context or {}
        self.on_event = on_event or noop
        self.on_exit = on_exit or noop
        self.error = error
        self._status = status or 'ready'

        self.proc = None
        self.out_file = None
        self.runned = False

        self.run_dir.ensure_dir()
        self.out_path = self.run_dir / 'out.log'
        self.meta_path = self.run_dir / 'meta.json'

        if not self.out_path.exists():
            self.out_file = self.out_path.open('w+', buffering = 1)

        if not self.meta_path.exists():
            self.save_meta()

    def __call__(self):
        if self.runned:
            raise RuntimeError('already runned')
        try:
            self.beg = now()
            self.status = 'running'
            self.save_meta()
            self.run()
        except:
            self.end = now()
            self.error = traceback.format_exc()
            self.status = 'error'
            traceback.print_exc()
        else:
            self.end = now()
            self.status = 'done'
        finally:
            self.save_meta()
            self.runned = True
            self.on_exit(self)

    def run(self):
        spec = self.run_spec
        run_type = spec['type']
        run_func = self.run_command
        run_args = {
            'cwd': spec.get('cwd') or str(self.run_dir),
            'env': {
                'PYTHONUNBUFFERED': '1', # ensure output is unbuffered
                **spec.get('env', {}),
            },
        }
        if run_type == 'command':
            run_args['cmd'] = spec['cmd']
        elif run_type == 'script':
            run_args['cmd'] = [sys.executable, spec['script'], *spec.get('args', [])]
        elif run_type == 'module':
            run_args['cmd'] = [sys.executable, '-m', spec['module'], *spec.get('args', [])]
        else:
            raise RuntimeError(f'unsupported runnable: {spec}')
        run_func(**run_args)

    def run_command(
            self,
            cmd,
            cwd = None,
            env = None,
    ):
        # TODO: handle orphan child process case
        use_shell = isinstance(cmd, str)
        self.proc = subprocess.Popen(
            cmd,
            cwd = cwd,
            env = {**os.environ, **env} if use_shell else env,
            stdout = subprocess.PIPE,
            stderr = subprocess.STDOUT, # redirect to stdout
            bufsize = 1,
            encoding = 'utf-8',
            universal_newlines = True,
            # `shell = True` is to support bash one liner
            # otherwise use `False` so `kill/terminate` can be done quickly
            shell = use_shell,
        )
        try:
            out_file = self.out_file
            for line in iter(self.proc.stdout.readline, ''):
                out_file.write(line)
        except KeyboardInterrupt:
            pass
        finally:
            self.proc.wait()
            if self.out_file:
                self.out_file.close()
                self.out_file = None
            self.returncode = self.proc.returncode
            if self.returncode != 0:
                if self.returncode < 0:
                    raise RuntimeError(f'run killed')
                elif self.returncode > 0:
                    raise RuntimeError(f'non zero return code: {self.returncode}')

    def save_meta(self):
        self.meta_path.save({
            'run_spec': self.run_spec,
            'id': self.id,
            'beg': self.beg.datetime_str() if self.beg else None,
            'end': self.end.datetime_str() if self.end else None,
            'status': self.status,
            'error': self.error,
        }, indent = 2)

    def kill(self):
        if self.proc:
            self.proc.kill()
            return True
        else:
            return False

    def terminate(self):
        if self.proc:
            self.proc.terminate()
            return True
        else:
            return False

    def info(self):
        return {
            'run_id': self.id,
            'status': self.status,
            'error': self.error,
            'beg': Timestamp.to_datetime_str(self.beg),
            'end': Timestamp.to_datetime_str(self.end),
        }

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
        self.on_event({
            'event': 'run_status_changed',
            'run_id': self.id,
            'status': self._status,
            'error': self.error,
            'beg': Timestamp.to_datetime_str(self.beg),
            'end': Timestamp.to_datetime_str(self.end),
        })

    @property
    def output(self) -> str:
        """
        Get output as a whole.

        Note: Partial output maybe got if still running.
        """
        with self.out_path.open() as f:
            return f.read()

    def iter_output(self) -> Iterable[str]:
        """
        Iterate over output line by line (without ending newline) synchronously.
        """
        with self.out_path.open() as f:
            while True:
                for line in iter(f.readline, ''):
                    yield line[:-1]
                _, _, error = select.select([f], [], [f], 0.01)
                if error or self.runned:
                    break

    async def iter_output_async(
        self,
        loop: 'asyncio.base_events.BaseEventLoop' = None,
    ) -> Iterable[str]:
        """
        Iterate over output line by line (without ending newline) asynchronously.
        """
        def collect():
            with self.out_path.open() as f:
                while True:
                    for line in iter(f.readline, ''):
                        loop.call_soon_threadsafe(que.put_nowait, line)
                    _, _, error = select.select([f], [], [f], 0.01)
                    if error or self.runned:
                        loop.call_soon_threadsafe(que.put_nowait, None)
                        break
        loop = loop or asyncio.get_event_loop()
        que = asyncio.Queue()
        thread = threading.Thread(target = collect)
        thread.start()
        while line := await que.get():
            if line is None:
                break
            yield line[:-1]

    def __del__(self):
        if self.out_file:
            self.out_file.close()
            self.out_file = None

    def __hash__(self):
        return hash(self.id)


class DummyRun:

    def __init__(self):
        self.run_spec = {}
        self.run_dir = None
        self.id = None
        self.beg = None
        self.end = None
        self.on_event = noop

        self.error = None
        self.proc = None
        self.out_file = None
        self.runned = False
        self.status = 'ready'

        self.out_path = None
        self.meta_path = None

    def __bool__(self):
        return False


# NOTE: using this on `subprocess.Popen(preexec_fn = ...)` will sometimes hang, don't know why.
# def exit_on_parent_exit():
#     try:
#         ctypes.cdll['libc.so.6'].prctl(1, signal.SIGHUP)
#     except:
#         pass
