import time
import uuid
import queue
import logging
import tempfile
import traceback
import threading
import functools
import multiprocessing
from pathlib import Path
from enum import Enum
from typing import Union, Callable, List, Iterable, Optional

import yaml
from fans.bunch import bunch
from fans.logger import get_logger, Logger

from fans.jober.sched import Sched
from fans.jober.target import Target
from fans.jober.job import Job
from fans.jober.run import Run
from fans.jober.capture import Capture


logger = get_logger(__name__)


class Jober:
    
    conf = {
        'conf_path': None,
        'root': None,
        'n_thread_pool_workers': 32,
        'timezone': 'Asia/Shanghai',
        'max_recent_runs': 3,
        'capture': 'default',
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
        self._events_thread = threading.Thread(target=self._collect_events, daemon=True)

        self._sched = Sched(
            n_threads=self.conf.n_thread_pool_workers,
            thread_pool_kwargs={
                'initializer': _init_pool_thread,
                'initargs': (self._events_queue,),
            },
            timezone=self.conf.timezone,
        )

        self._listeners = set()
        
        self._load_jobs_from_conf()

    def start(self):
        if not self.started:
            self._sched.start()
            self._events_thread.start()
            Capture.enable_proxy()
            self.started = True

    def stop(self):
        if self.started:
            self._sched.stop()
            Capture.disable_proxy()
            self.started = False
    
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
    def jobs(self) -> Iterable[Job]:
        for job in self._id_to_job.values():
            yield job

    def run_job(self, *_args, **_kwargs) -> Job:
        """
        Run new job:
        
            jober.run_job('date')
        
        Run existing job:

            job = jober.get_job('<job_id>')
            jober.run_job(job)
        """
        if _args and isinstance(_args[0], Job):
            job = _args[0]
            run_args = _kwargs.get('args')
            run_kwargs = _kwargs.get('kwargs')
        else:
            _kwargs.setdefault('volatile', True)
            job = self.add_job(*_args, **_kwargs)
            run_args = None
            run_kwargs = None

        run = job.new_run(args=run_args, kwargs=run_kwargs)
        run.native_id = self._sched.run_singleshot(run, **job._apscheduler_kwargs)

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

    def make_job(
            self,
            target: Union[str, Callable],
            args: tuple = (),
            kwargs: dict = {},
            *,
            cwd: str = None,
            shell: bool = False,
            process: bool = False,
            **job_kwargs,
    ) -> 'Job':
        """Make a job without adding to jober."""
        target = Target.make(
            target,
            args,
            kwargs,
            shell=shell,
            cwd=cwd,
            process=process,
        )

        job_kwargs.setdefault('max_recent_runs', self.conf.max_recent_runs)
        job_kwargs.setdefault('on_event', lambda event: self._events_queue.put(event))
        job_kwargs.setdefault('root_work_dir', self.work_dir)
        job_kwargs.setdefault('capture', self.conf.capture)

        job = Job(target, **job_kwargs)

        return job
    
    def get_job(self, job_id: str) -> Optional[Job]:
        return self._id_to_job.get(job_id)
    
    def prune_jobs(self) -> list[Job]:
        pruned = []
        for job_id in [d.id for d in self.jobs if d.volatile]:
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
    
    @property
    def work_dir(self):
        return Path(self.conf.root).expanduser()
    
    def as_dict(self):
        return {**self.conf}
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, *_, **__):
        self.stop()
    
    def _schedule_job(self, job, when):
        if isinstance(when, (int, float)):
            self._sched.run_interval(job, when, **job._apscheduler_kwargs)
        elif isinstance(when, str):
            self._sched.run_cron(job, when, **job._apscheduler_kwargs)
        else:
            raise NotImplementedError(f'unsupported when: {when}')
    
    def _add_job(self, job):
        self._id_to_job[job.id] = job

    def _collect_events(self):
        queue = self._events_queue
        while True:
            event = queue.get()
            for listener in self._listeners:
                try:
                    listener(event)
                except:
                    traceback.print_exc()
    
    def _load_jobs_from_conf(self):
        for spec in self.conf.get('jobs', []):
            spec = _normalized_job_spec(spec)
            name = spec.get('name')
            self.add_job(
                target=spec.get('executable'),
                id=name,
                name=name,
                cwd=spec.get('cwd'),
                when=spec.get('when'),
                shell=spec.get('shell'),
            )


def _prepare_config(conf_path, conf: dict):
    if conf_path:
        conf['conf_path'] = conf_path

    # maybe load from file
    if conf.get('conf_path'):
        fpath = Path(conf['conf_path']).expanduser()
        logger.info(f"loading config from {fpath}")
        with fpath.open() as f:
            conf.update(yaml.safe_load(f) or {})

    # set missing defaults
    for key, value in Jober.conf.items():
        conf.setdefault(key, value)
    
    # specialized defaults
    if conf['root'] is None:
        conf['root'] = tempfile.gettempdir()

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


def _init_pool_thread(queue: queue.Queue):
    global _events_queue
    _events_queue = queue
    Logger.reset_handlers(module_levels={'apscheduler': logging.WARNING})


_events_queue = None
