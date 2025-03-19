import time
import uuid
import queue
import inspect
import traceback
import threading
import functools
import multiprocessing
from pathlib import Path
from enum import Enum
from typing import Union, Callable, List, Iterable, Optional

import yaml
from fans.bunch import bunch
from fans.logger import get_logger

from .sched import Sched
from .target import Target, TargetType
from . import util
from .job.job import Job, Run
from .event import RunEventer


logger = get_logger(__name__)


DEFAULT_MODE = 'thread'


class Jober:
    
    env = bunch({
        'conf_path': None,
        'n_thread_pool_workers': 32,
    })

    _instance = None

    @staticmethod
    def get_instance():
        if Jober._instance is None:
            Jober._instance = Jober()
        return Jober._instance

    def __init__(self, env: bunch = None):
        self.conf = _conf_from_env(env or Jober.env)

        self._id_to_job = {}
        self._events_queue = queue.Queue()

        self._sched = Sched(
            n_threads=self.conf.n_thread_pool_workers,
            thread_pool_kwargs={
                'initializer': _init_pool,
                'initargs': (self._events_queue,),
            },
        )

        self._thread_events_thread = threading.Thread(target=self._collect_events, daemon=True)

        self._listeners = set()

        self.started = False
    
    @property
    def info(self) -> dict:
        return {
            **self.conf,
        }
    
    @property
    def jobs(self) -> Iterable[Job]:
        for job in self._id_to_job.values():
            yield job
    
    @property
    def job_ids(self) -> Iterable[str]:
        for job in self.jobs:
            yield job.id

    def run_job(
            self,
            *args,
            **kwargs,
    ) -> 'Run':
        if isinstance(args[0], Job):
            job = args[0]
        else:
            job = self.add_job(*args, **kwargs)
        run = job.new_run()
        self._sched.run_singleshot(self._make_job_for_run(run, job))
        return run

    def add_job(
            self,
            *args,
            sched: int|float|str = None,
            initial_run: bool = True,
            **kwargs,
    ) -> Job:
        """Make a job and add to jober."""
        job = self.make_job(*args, **kwargs)
        self._id_to_job[job.id] = job
        
        if sched is not None:
            if isinstance(sched, (int, float)):
                interval = sched
                self._sched.run_interval(job, interval)
            else:
                raise NotImplementedError(f'unsupported sched: {sched}')

        self.start()  # ensure started

        return job
    
    def prune_jobs(self) -> list[Job]:
        pruned = []
        for job_id in list(self.job_ids):
            job = self.remove_job(job_id)
            if job:
                pruned.append(job)
        return pruned

    def remove_job(self, job_id: str) -> Optional[Job]:
        job = self.get_job(job_id)
        if not job:
            logger.warning(f'remove_job: job ID not found {job_id}')
            return None
        if not job.removable:
            logger.warning(f'remove_job: job not removable {job_id}')
            return None
        del self._id_to_job[job_id]
        return job

    def run_for_a_while(self, seconds: float = 0.001):
        time.sleep(seconds)

    # TODO: sched can be separated out from Job?
    def make_job(
            self,
            target: Union[str, Callable],
            args: tuple = (),
            kwargs: dict = {},
            *,
            id: str = None,
            name: str = None,
            extra: any = None,
            sched: str = None,
            **__,
    ) -> 'Job':
        """
        Make a job without adding to jober.

        target: Union[str, Callable]
        args: tuple = None
        kwargs: dict = None
        sched: str = None
        """
        target = Target.make(target, args, kwargs)
        job = Job(
            target,
            id=id,
            name=name,
            extra=extra,
        )
        return job

    def start(self):
        if not self.started:
            self._sched.start()
            self._thread_events_thread.start()
            util.enable_proxy()
            self.started = True

    def stop(self):
        if self.started:
            self._sched.stop()
            util.disable_proxy()
            self.started = False

    def get_job(self, job_id: str) -> 'Job':
        """
        Get job by ID.

        Params:
            job_id - ID of the job.

        Returns:
            Job with given ID or None if not found
        """
        return self._id_to_job.get(job_id)

    def iter_jobs(
            self,
            status: str = None,
            mode: str = None,
    ) -> Iterable['Job']:
        """
        Get an iterable of jobs.

        Params:
            status - Filter with given status
            mode - Filter with given mode
        """
        jobs = self._id_to_job.values()
        for job in jobs:
            if mode and job.mode != mode:
                continue
            if status and job.status != status:
                continue
            yield job

    def get_jobs(self, *args, **kwargs) -> List['Job']:
        """
        Get all jobs. See `iter_jobs` for filter/sort options.
        """
        return list(self.iter_jobs(*args, **kwargs))

    def add_listener(self, callback: Callable[[dict], None]) -> any:
        """
        Add an event listener to listen for all events.

        Params:
            callback - Callback called with the event

        Returns:
            token - Token used to unlisten the added event listener
        """
        listeners = set(self._listeners)
        listeners.add(callback)
        self._listeners = listeners
        return callback

    def remove_listener(self, token: any):
        """
        Remove previously added event listener.

        Params:
            token - Token got from `add_listener` return value.
        """
        listeners = set(self._listeners)
        listeners.discard(token)
        self._listeners = listeners

    def _collect_events(self):
        queue = self._events_queue
        while True:
            event = queue.get()
            job_id = event['job_id']
            job = self._id_to_job.get(job_id)
            if not job:
                logger.warning(
                    f'got job event for job with id "{job_id}" '
                    f'but the job is not known'
                )
                continue
            job._on_run_event(event)

            for listener in self._listeners:
                try:
                    listener(event)
                except:
                    traceback.print_exc()

    def _make_job_for_run(self, run, job):
        def _run():
            return _run_job(**{
                'target': job.target,
                'job_id': run.job_id,
                'run_id': run.run_id,
                'prepare': lambda: _prepare_thread_run(
                    self._events_queue, run.job_id, run.run_id,
                    module_logging_levels=self._sched.module_logging_levels,
                )
            })
        return _run


def _init_pool(queue: queue.Queue):
    global _events_queue
    _events_queue = queue


def _run_job(*, target, job_id, run_id, prepare):
    eventer = RunEventer(job_id=job_id, run_id=run_id)
    try:
        _events_queue.put(eventer.begin())
        if prepare:
            prepare()
        _consumed(target())
    except:
        print(traceback.format_exc()) # output traceback in job run thread
        _events_queue.put(eventer.error())
    else:
        _events_queue.put(eventer.done())


def _prepare_thread_run(thread_out_queue, job_id, run_id, module_logging_levels={}):
    util.redirect(
        queue = thread_out_queue,
        job_id = job_id,
        run_id = run_id,
        module_logging_levels = module_logging_levels,
    )


def _consumed(value):
    if inspect.isgenerator(value):
        # ensure a generator function is iterated
        for _ in value:
            pass


def _conf_from_env(env: bunch):
    conf = bunch(env)
    if env.conf_path:
        with Path(env.conf_path).open() as f:
            conf.update(yaml.safe_load(f))
    return conf


_events_queue = None
