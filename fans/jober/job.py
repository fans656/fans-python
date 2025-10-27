import uuid
import time
import queue
import asyncio
from collections import deque
from typing import Iterable, Optional

from fans.fn import noop
from fans.logger import get_logger

from .run import Run, DummyRun, dummy_run


logger = get_logger(__name__)


class Job:
    
    @staticmethod
    def from_dict(spec: dict):
        pass

    def __init__(
            self,
            target: any,
            id: str = None,
            name: str = None,
            extra: any = None,
            max_instances: int = 1,
            max_recent_runs: int = 3,
            disabled: bool = False,
            volatile: bool = False,
            stdout: str = ':memory:',
            stderr: str = ':stdout:',
            on_event=noop,
    ):
        self.target = target
        self.id = id or uuid.uuid4().hex
        self.job_id = self.id
        self.name = name
        self.extra = extra
        
        self.max_instances = max_instances
        self.max_recent_runs = max_recent_runs
        self.disabled = disabled
        self.volatile = volatile
        self.stdout = stdout
        self.stderr = stderr
        self.on_event = on_event

        self._id_to_run = {}
        self._recent_runs = deque([])
    
    def __call__(self, args=None, kwargs=None):
        run = self.new_run(args=args, kwargs=kwargs)
        return run()
    
    def disable(self):
        self.disabled = True
    
    def enable(self):
        self.disabled = False
    
    def as_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'extra': self.extra,
        }

    @property
    def status(self) -> str:
        return self.last_run.status

    @property
    def trace(self) -> str:
        return self.last_run.trace

    @property
    def output(self) -> str:
        return self.last_run.output

    @property
    def runs(self) -> Iterable['Run']:
        return self._id_to_run.values()

    @property
    def removable(self):
        if not self.runs:
            return True
        if self.finished:
            return True
        return False

    @property
    def finished(self):
        return self.last_run.finished

    @property
    def last_run(self):
        return self._recent_runs and self._recent_runs[-1] or dummy_run

    @property
    def source(self) -> str:
        return self.target.source
    
    def get_run(self, run_id: str) -> Optional[Run]:
        return self._id_to_run.get(run_id)

    def new_run(self, args=None, kwargs=None):
        if self.disabled:
            return DummyRun(job_id=self.id)

        job_id = self.id
        run_id = uuid.uuid4().hex
        run = Run(
            target=self.target,
            job_id=job_id,
            run_id=run_id,
            args=args,
            kwargs=kwargs,
            stdout=self.stdout,
            stderr=self.stderr,
            on_event=self.on_event,
        )

        self._id_to_run[run_id] = run
        self._recent_runs.append(run)

        self._clear_obsolete_runs()

        return run
    
    def wait(self, interval=0.01):
        while self.last_run.status in ('init', 'running'):
            time.sleep(interval)
    
    @property
    def _apscheduler_kwargs(self):
        ret = {
            'max_instances': self.max_instances,
        }
        return ret
    
    def _clear_obsolete_runs(self):
        while len(self._recent_runs) > self.max_recent_runs:
            run = self._recent_runs.popleft()
            del self._id_to_run[run.run_id]
