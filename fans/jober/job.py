import uuid
import time
import queue
import asyncio
from typing import Iterable, Optional

from fans.logger import get_logger

from .run import Run, dummy_run


logger = get_logger(__name__)


class Job:

    def __init__(
            self,
            target: any,
            id: str = None,
            name: str = None,
            extra: any = None,
    ):
        self.target = target
        self.id = id or uuid.uuid4().hex
        self.job_id = self.id
        self.name = name
        self.extra = extra

        self._id_to_run = {}
        self._last_run_id = None
        self._max_run_time = 0
    
    def __call__(self, *args, **kwargs):
        run = self.new_run()
        return run(*args, **kwargs)
    
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
    def output_lines(self) -> str:
        return self.last_run.output_lines

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
        return self.get_run(self._last_run_id) or dummy_run

    @property
    def source(self) -> str:
        return self.target.source
    
    def get_run(self, run_id: str) -> Optional[Run]:
        return self._id_to_run.get(run_id)

    def new_run(self):
        run_id = uuid.uuid4().hex
        run = Run(
            target=self.target,
            job_id=self.id,
            run_id=run_id,
        )
        self._id_to_run[run_id] = run
        return run
    
    def wait(self, interval=0.01):
        while self.last_run.status in ('init', 'running'):
            time.sleep(interval)

    def _on_run_event(self, event):
        run_id = event['run_id']

        if run_id not in self._id_to_run:
            return

        if event['type'] == 'job_run_begin' and event['time'] > self._max_run_time:
            self._last_run_id = run_id

        run = self._id_to_run[run_id]
        run._on_run_event(event)
