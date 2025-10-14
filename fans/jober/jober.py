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
from .target import Target
from . import util
from .job import Job
from .run import Run
from .event import RunEventer


logger = get_logger(__name__)


DEFAULT_MODE = 'thread'


class Jober:
    
    conf = {
        'conf_path': None,
        'capture': 'memory',
        'n_thread_pool_workers': 32,
        'timezone': 'Asia/Shanghai',
        'jobs': [],
    }

    _instance = None

    @classmethod
    def set_instance(cls, instance):
        cls._instance = instance

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = Jober(**cls.conf)
        return cls._instance

    def __init__(self, conf_path=None, **conf):
        self.conf = _prepare_config(conf_path, conf)
        self.started = False

        self._id_to_job = {}
        self._events_queue = queue.Queue()

        self._sched = Sched(
            n_threads=self.conf.n_thread_pool_workers,
            thread_pool_kwargs={
                'initializer': _init_pool,
                'initargs': (self._events_queue,),
            },
            timezone=self.conf.timezone,
        )

        self._thread_events_thread = threading.Thread(target=self._collect_events, daemon=True)
        self._listeners = set()
        
        self._load_jobs_from_conf()
    
    def wait(self, timeout: float = None):
        try:
            beg = time.time()
            while True:
                if timeout and time.time() - beg >= timeout:
                    break
                time.sleep(0.01)
        except KeyboardInterrupt:
            pass
    
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

    def run_job(self, *args, **kwargs) -> 'Run':
        """
        Sample usages:
        
            jober.run_job('sleep 1 && date', shell=True)
        """
        if isinstance(args[0], Job):
            job = args[0]
        else:
            job = self.add_job(*args, **kwargs)

        self._sched.run_singleshot(self._prepare_run(job, **kwargs), **job._apscheduler_kwargs)

        return job

    def add_job(
            self,
            *args,
            when: int|float|str = None,
            initial_run: bool = True,
            **kwargs,
    ) -> Job:
        """Make a job and add to jober."""
        job = self.make_job(*args, **kwargs)

        self._add_job(job)
        
        if when is not None:
            self._schedule_job(job, when)

        self.start()  # ensure started

        return job
    
    def disable_job(self, job):
        pass  # TODO
    
    def enable_job(self, job):
        pass  # TODO
    
    def _schedule_job(self, job, when):
        if isinstance(when, (int, float)):
            self._sched.run_interval(self._prepare_run(job), when, **job._apscheduler_kwargs)
        elif isinstance(when, str):
            self._sched.run_cron(self._prepare_run(job), when, **job._apscheduler_kwargs)
        else:
            raise NotImplementedError(f'unsupported when: {when}')
    
    def _add_job(self, job):
        self._id_to_job[job.id] = job
    
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

    def make_job(
            self,
            target: Union[str, Callable],
            args: tuple = (),
            kwargs: dict = {},
            *,
            id: str = None,
            name: str = None,
            extra: any = None,
            cwd: str = None,
            shell: bool = False,
            process: bool = False,
            **job_kwargs,
    ) -> 'Job':
        """
        Make a job without adding to jober.

        target: Union[str, Callable]
        args: tuple = None
        kwargs: dict = None
        """
        target = Target.make(
            target,
            args,
            kwargs,
            shell=shell,
            cwd=cwd,
            process=process,
        )
        job = Job(
            target,
            id=id,
            name=name,
            extra=extra,
            **job_kwargs,
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

    def _prepare_run(self, job, args=None, kwargs=None, **__):
        run = job.new_run()

        def _run():
            if args is not None or kwargs is not None:
                target = job.target.bind(args or [], kwargs or {})
            else:
                target = job.target
            
            if self.conf.capture:
                prepare = lambda: _prepare_thread_run(
                    self._events_queue, run.job_id, run.run_id,
                    module_logging_levels=self._sched.module_logging_levels,
                )
            else:
                prepare = None

            return _run_job(**{
                'target': target,
                'job_id': run.job_id,
                'run_id': run.run_id,
                'prepare': prepare,
            })
        return _run
    
    def _load_jobs_from_conf(self):
        for spec in self.conf.get('jobs', []):
            spec = _normalized_job_spec(spec)
            name = spec.get('name')
            self.add_job(
                target=spec.get('executable'),
                id=name,
                name=name,
                cwd=spec.get('cwd'),
                shell=spec.get('shell'),
            )


def _prepare_config(conf_path, conf: dict):
    if conf_path:
        conf['conf_path'] = conf_path

    # set default for missing values
    for key, value in Jober.conf.items():
        conf.setdefault(key, value)

    # maybe load from file
    if conf.get('conf_path'):
        fpath = Path(conf['conf_path']).expanduser()
        logger.info(f"loading config from {fpath}")
        with fpath.open() as f:
            conf.update(yaml.safe_load(f) or {})

    return bunch(conf)


def _normalized_job_spec(spec: dict):
    executable = spec.get('cmd')
    if not executable:
        executable = spec.get('module')
    if not executable:
        executable = spec.get('script')
    return {
        **spec,
        'executable': executable,
    }


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


_events_queue = None
