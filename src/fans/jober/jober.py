import json
import pathlib
import threading
from typing import List, Union, Optional

import pytz
from fans.path import Path
from fans.logger import get_logger
from fans.pubsub import PubSub
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from . import errors
from .job import Job
from .utils import load_spec


SpecSource = Optional[Union[dict, pathlib.Path, str]]
RunID = str
logger = get_logger(__name__)


class Jober:
    """
    Instance to manage all jobs.
    """

    spec: SpecSource = None # specify the jober spec
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
                    'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                    'max_workers': 20, # TODO: changable
                },
            },
            timezone = pytz.timezone('Asia/Shanghai'),
        )

        self.spec = spec = load_spec(spec)
        self.context = spec.get('context', {})
        for job_spec in spec.get('jobs', []):
            self.make_and_add_job(job_spec)

    def get_job_by_id(self, id: str) -> Optional[Job]:
        """
        Get a job by its ID.

        Returns:
            The job or None if no job exists for given ID.
        """
        return self._id_to_job.get(id)

    @property
    def jobs(self) -> List[Job]:
        """
        Get all existing jobs.
        """
        return self._jobs

    def run_job(self, id: str, args: str = None) -> RunID:
        """
        Run a job on demand.

        Args:
            id - the job ID.
            args - arguments for this job run.

        Returns:
            RunID of this run.
        """
        job = self.get_job_by_id(id)
        if not job:
            raise errors.NotFound(f'"{id}" not found')
        # TODO: passing args
        self.sched.add_job(job, DateTrigger())

    def make_job(self, spec: dict) -> Job:
        """
        Make a new job given the job spec.
        """
        return Job(
            context = {
                **self.context,
            },
            root_dir = self.context.get('root_dir'),
            on_event = self.pubsub.publish,
            **spec,
        )

    def add_job(self, job: Job):
        """
        Add a new job to jober.
        """
        if job.id in self._id_to_job:
            raise errors.Conflict(f'"{job.name}" already exists')
        self._jobs.append(job)
        self._id_to_job[job.id] = job
        self._maybe_schedule_job(job)

    def make_and_add_job(self, spec: dict):
        """
        Make and add job to jober given a job spec.
        """
        self.add_job(self.make_job(spec))

    def start(self):
        """
        Start the jober.
        """
        self.sched.start()

    def stop(self):
        """
        Stop the jober.
        """
        self.sched.shutdown()

    def _maybe_schedule_job(self, job):
        sched_spec = job.sched
        if not sched_spec:
            return
        if isinstance(sched_spec, int):
            seconds = sched_spec
            if seconds > 0:
                # TODO: access to this job sched when seconds is like 3600
                self.sched.add_job(job, DateTrigger()) # first run
            job.sched_job = self.sched.add_job(job, IntervalTrigger(seconds = seconds))
        # TODO: other triggers
        else:
            logger.warning(f'unsupported sched: {repr(sched_spec)}')
