import uuid
import tempfile
import traceback
from typing import Callable

from fans.path import Path
from fans.datelib import now


class BaseRun:
    """
    Represent a single execution of a job.

    Following types of execution are supported:
        command line - e.g. `for i in 1 2 3; do echo $i; sleep 1; done`
        module - e.g. `python -m quantix.pricer`
        script - e.g. `python t.py`
        callable - in code function as job

    Before the run start, it will create a run directory and prepare following files:
        <run_dir>/meta.json - meta info of this run
        <run_dir>/out.log - output of this run
    If run directory is not given, temporary directory will be used.

    Following events will happen during the run:
        job_run_status_changed - status change
    """

    def __init__(
            self,
            runnable: 'Runnable',
            run_id: str = None,
            run_dir: pathlib.Path = None,
            on_event: Callable[[dict], None] = None,
    ):
        self.runnable = runnable
        self.run_id = run_id or uuid.uuid4().hex
        self.run_dir = Path(run_dir) or Path(tempfile.gettempdir()) / self.run_id
        self.on_event = on_event

        self.meta_path = self.run_dir / 'meta.json'

        self.beg = None
        self.end = None
        self.error = None
        self._status = None

    def __call__(self):
        try:
            self.run_dir.ensure_dir()
            self.beg = now()
            self.status = 'running'
            self.save_meta()
            self.run()
        except:
            self.status = 'error'
            self.error = traceback.format_exc()
            traceback.print_exc()
        else:
            self.status = 'done'
        finally:
            self.end = now()
            self.save_meta()

    def run(self):
        pass

    def save_meta(self):
        self.meta_path.save(self.meta)

    @property
    def meta(self):
        return {
            'id': self.run_id,
            'beg': self.beg.datetime_str() if self.beg else None,
            'end': self.end.datetime_str() if self.end else None,
            'status': self.status,
            'error': self.error,
        }

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status
        self.on_event({
            'event': 'job_run_status_changed',
            'run_id': self.run_id,
            'status': self._status,
        })


def normalized_run_spec(spec: dict):
    """
    Valid samples of run spec:

        {
            'type': 'command',
            'cmd': 'for i in 1 2 3; echo $i; sleep 1; done',
            # optional
            'cwd': '/home/fans656',
            # optional
            'env': {'foo': 3, 'bar': 5},
        }
    """
    pass
