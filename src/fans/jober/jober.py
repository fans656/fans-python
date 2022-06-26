import json
import threading
from typing import List, Union, Optional

import pytz
from fans.path import Path
from fans.logger import get_logger
from fans.pubsub import PubSub
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from . import errors
from .job import Job
from .utils import load_spec


SpecSource = Union[dict, Path, str]
RunID = str
logger = get_logger(__name__)


class Jober:

    spec: SpecSource = None # FastAPI using app can override this to load spec
    _instance: 'Jober' = None

    @classmethod
    def get_instance(cls, spec: SpecSource = None):
        if cls._instance is None:
            cls._instance = cls(spec or cls.spec)
        return cls._instance

    def __init__(self, spec: SpecSource = None):
        self._jobs = []
        self._id_to_job = {}

        self.pubsub = PubSub()

        self.sched = BackgroundScheduler(
            executors = {
                'default': {
                    'type': 'threadpool',
                    'max_workers': 20,
                },
            },
            timezone = pytz.timezone('Asia/Shanghai'),
        )

        spec = load_spec(spec)
        self.context = spec.get('context', {})
        for job_spec in spec.get('jobs', []):
            self.make_and_add_job(job_spec)

    def get_job_by_id(self, id: str) -> Optional[Job]:
        return self._id_to_job.get(id)

    @property
    def jobs(self) -> List[Job]:
        return self._jobs

    def run_job(self, id: str, args: str) -> RunID:
        job = self.get_job_by_id(id)
        if not job:
            raise errors.NotFound(f'"{id}" not found')
        # TODO: passing args
        thread = threading.Thread(target = job)
        thread.start()

    def make_job(self, spec: dict) -> Job:
        return Job(spec = spec, context = {
            'pubsub': self.pubsub,
            **self.context,
        })

    def add_job(self, job: Job) -> bool:
        if job.name in self._id_to_job:
            raise errors.Conflict(f'"{job.name}" already exists')
        self._jobs.append(job)
        self._id_to_job[job.id] = job
        self.schedule_job(job)

    def schedule_job(self, job):
        sched_spec = job.spec.get('sched')
        if not sched_spec:
            return
        if isinstance(sched_spec, int):
            seconds = sched_spec
            job.sched_job = self.sched.add_job(job, 'interval', seconds = seconds)
        # TODO: other triggers
        else:
            logger.warning(f'unsupported sched: {repr(sched_spec)}')

    def make_and_add_job(self, spec: dict):
        self.add_job(self.make_job(spec))

    def start(self):
        self.sched.start()

    def stop(self):
        self.sched.shutdown()
