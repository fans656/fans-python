import uuid
import queue
import asyncio
from abc import abstractmethod
from typing import Iterable

from fans.logger import get_logger

from .run import Run, dummy_run


logger = get_logger(__name__)


class Job:
    """
    Base impl, see separate concrete impl for details.
    """

    mode = None

    def __init__(
            self,
            target: any,
            name: str = None,
            extra: any = None,
    ):
        self.target = target
        self.id = uuid.uuid4().hex
        self.name = name
        self.extra = extra

        self._id_to_run = {}
        self._last_run_id = None
        self._max_run_time = 0

        self.init()

    @abstractmethod
    def init(self):
        pass

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
        return self._id_to_run.get(self._last_run_id) or dummy_run

    @property
    def source(self) -> str:
        return self.target.source

    def new_run(self):
        run_id = uuid.uuid4().hex
        run = Run(
            job_id = self.id,
            run_id = run_id,
        )
        self._id_to_run[run_id] = run
        return run

    def _on_run_event(self, event):
        run_id = event['run_id']

        if run_id not in self._id_to_run:
            return

        if event['type'] == 'job_run_begin' and event['time'] > self._max_run_time:
            self._last_run_id = run_id

        run = self._id_to_run[run_id]
        run._on_run_event(event)
