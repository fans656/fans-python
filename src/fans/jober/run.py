import sys
import uuid
import select
import asyncio
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
        return cls(**(path / 'meta.json').load())

    def __init__(
            self,
            run_spec: dict = None,
            id: str = None,
            beg: Union[str, Timestamp] = None,
            end: Union[str, Timestamp] = None,
            run_dir: str = None,
            out_path: str = None,
            on_event: Callable[[dict], None] = None,
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
        self.id = id or uuid.uuid4().hex

        self.beg = Timestamp.from_datetime_str(beg)
        self.end = Timestamp.from_datetime_str(end)

        self.run_dir = Path(run_dir or tempfile.gettempdir())
        self.out_path = out_path or self.run_dir / 'out.log'
        self.meta_path = self.run_dir / 'meta.json'

        self._status = 'init'
        self.on_event = on_event or noop
        self.finished = False

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
        self.on_event({
            'event': 'run_status_changed',
            'id': self.id,
            'status': self._status,
        })

    @property
    def output(self) -> str:
        """
        Get output as a whole.

        Note: Partial output maybe got if the running is not finished.
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
                if error or self.finished:
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
                    if error or self.finished:
                        break
        loop = loop or asyncio.get_event_loop()
        que = asyncio.Queue()
        thread = threading.Thread(target = collect)
        thread.start()
        while line := await que.get():
            yield line[:-1]

    def __call__(self):
        try:
            self.beg = now()
            self.status = 'running'
            self.save_meta()
            self.run()
        except:
            self.status = 'error'
            traceback.print_exc()
        else:
            self.status = 'done'
        finally:
            self.end = now()
            self.save_meta()

    def run(self):
        spec = self.run_spec
        run_type = spec['type']
        run_func = self.run_command
        run_args = {
            'cwd': spec.get('cwd'),
            'env': {
                'PYTHONUNBUFFERED': '1', # ensure output is unbuffered
                **spec.get('env'),
            },
        }
        if run_type == 'command':
            run_args['cmd'] = spec['cmd']
        elif run_type == 'script':
            run_args['cmd'] = [sys.executable, spec['script'], *spec['args']]
        elif run_type == 'module':
            run_args['cmd'] = [sys.executable, '-m', spec['module'], *spec['args']]
        else:
            raise RuntimeError(f'unsupported runnable: {spec}')
        run_func(**run_args)

    def run_command(
            self,
            cmd,
            cwd = None,
            env = None,
    ):
        if isinstance(cmd, list):
            cmd = ' '.join(cmd)
        with self.out_path.open('w+', buffering = 1) as out_file:
            self.proc = subprocess.Popen(
                cmd,
                cwd = cwd,
                env = env,
                stdout = subprocess.PIPE,
                stderr = subprocess.STDOUT, # redirect to stdout
                bufsize = 1,
                encoding = 'utf-8',
                universal_newlines = True,
                shell = True, # to support bash one liner
            )
            try:
                for line in iter(self.proc.stdout.readline, ''):
                    out_file.write(line)
            except KeyboardInterrupt:
                pass
            finally:
                self.proc.wait()
                self.finished = True
                self.proc = None

    def save_meta(self):
        self.meta_path.save({
            'id': self.id,
            'run': self.run_spec,
            'beg': self.beg.datetime_str() if self.beg else None,
            'end': self.end.datetime_str() if self.end else None,
            'status': self.status,
        }, indent = 2)


# NOTE: using this on `subprocess.Popen(preexec_fn = ...)` will sometimes hang, don't know why.
# def exit_on_parent_exit():
#     try:
#         ctypes.cdll['libc.so.6'].prctl(1, signal.SIGHUP)
#     except:
#         pass
