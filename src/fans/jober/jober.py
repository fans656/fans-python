import time
import uuid
import queue
import traceback
import threading
import functools
import multiprocessing as mp
from pathlib import Path
from enum import Enum
from typing import Union, Callable, List, Iterable

from fans.bunch import bunch
from fans.logger import get_logger

from .sched import make_sched
from .target import Target, TargetType


logger = get_logger(__name__)


class Jober:

    def __init__(self, **conf_spec):
        """
        See `make_conf` for args doc
        """
        self.conf = conf = make_conf(**conf_spec)

        self.root = conf.root
        self.n_threads = conf.n_threads

        self._id_to_job = {}
        self._mp_queue = mp.Queue()
        self._th_queue = queue.Queue()

        self._sched = make_sched(**{
            **conf,
            'thread_pool_kwargs': {
                'initializer': _init_pool,
                'initargs': (self._th_queue,),
            },
            'process_pool_kwargs': {
                'initializer': _init_pool,
                'initargs': (self._mp_queue,),
            },
        })

        self._process_events_thread = threading.Thread(
            target = functools.partial(self._collect_events, self._mp_queue), daemon = True)

        self._thread_events_thread = threading.Thread(
            target = functools.partial(self._collect_events, self._th_queue), daemon = True)

    def run_job(self, *args, **kwargs) -> 'Job':
        job = self.add_job(*args, **kwargs)
        self._sched.run_singleshot(_run_job, (job.id, job.target), mode = job.mode)
        return job

    def add_job(self, *args, **kwargs) -> 'Job':
        """
        Make a job and add to jober.
        """
        job = self.make_job(*args, **kwargs)
        self._id_to_job[job.id] = job
        return job

    # TODO: mode should not be in Job
    # TODO: sched can be separated out from Job?
    def make_job(
            self,
            target: Union[str, Callable],
            args: tuple = (),
            kwargs: dict = {},
            *,
            mode: str = None,
            sched: str = None,
    ) -> 'Job':
        """
        Make a job without adding to jober.

        target: Union[str, Callable]
        args: tuple = None
        kwargs: dict = None
        mode: str = None - 'thread'|'process'
        sched: str = None
        """
        target = Target.make(target, args, kwargs)
        if target.type == TargetType.command:
            make = self._make_process_job
        else:
            make = self._get_job_maker_by_mode(mode)
        return make(target)

    def start(self):
        self._sched.start()
        self._thread_events_thread.start()
        self._process_events_thread.start()

    def stop(self):
        self._sched.stop()

    def get_jobs(self, *args, **kwargs) -> List['Job']:
        return list(self.iter_jobs(*args, **kwargs))

    def iter_jobs(
            self,
            status: str = None,
            mode: str = None,
    ) -> Iterable['Job']:
        jobs = self._id_to_job.values()
        for job in jobs:
            if mode and job.mode != mode:
                continue
            if status and job.status != status:
                continue
            yield job

    def _get_job_maker_by_mode(self, mode: str):
        match mode:
            case 'process':
                return self._make_process_job
            case 'thread':
                return self._make_thread_job
            case _:
                return (
                    conf_default.default_mode == 'thread' and
                    self._make_thread_job or
                    self._make_process_job
                )

    def _make_thread_job(self, target: 'FuncTarget') -> 'Job':
        from .job.thread_job import ThreadJob
        return ThreadJob(target)

    def _make_process_job(self, target: 'ProcTarget') -> 'Job':
        from .job.process_job import ProcessJob
        return ProcessJob(target)

    def _collect_events(self, queue):
        while True:
            event = queue.get()
            job = self._id_to_job.get(event['job_id'])
            if not job:
                logger.warning(
                    f'got job event for job with id "{event["job_id"]}" '
                    f'but the job is not known'
                )
                continue
            job._on_run_event(event)


def make_conf(
        conf_path: str = ...,
        root: str = ...,
        default_mode: str = ...,
        n_threads: int = ...,
        n_processes: int = ...,
):
    """
    conf_path: str - config yaml path, read config dict from the yaml content
    root: str - root directory path, intermediate files will be store here
    default_mode: str - default execution mode to use ('thread'/'process')
    n_threads: int - number of threads to use for thread pool
    n_processes: int - number of processes to use for process pool
    """
    conf = {}

    if conf_path is not ...:
        import yaml
        try:
            with Path(conf_path).open() as f:
                conf = yaml.safe_load(f)
                assert isinstance(conf, dict)
        except Exception as exc:
            logger.warning(f'error reading conf from "{conf_path}": {exc}')
            conf = {}

    eor = lambda val, name: conf.get(name) if val is ... else val

    return bunch(
        root = eor(root, 'root'),
        default_mode = eor(default_mode, 'default_mode') or conf_default.default_mode,
        n_threads = eor(n_threads, 'n_threads') or conf_default.n_threads,
        n_processes = eor(n_processes, 'n_processes') or conf_default.n_processes,
    )


class conf_default:

    default_mode = 'process'
    n_threads = 4
    n_processes = 4


class RunEvent:

    def __init__(self, job_id):
        self.job_id = job_id
        self.run_id = uuid.uuid4().hex

    def begin(self):
        return self._event('job_run_begin')

    def done(self):
        return self._event('job_run_done')

    def error(self):
        return self._event('job_run_error', trace = traceback.format_exc())

    def _event(self, event_type, **data):
        return {
            'type': event_type,
            'job_id': self.job_id,
            'run_id': self.run_id,
            'time': time.time(),
            **data,
        }


def _init_pool(queue: 'queue.Queue|multiprocessing.Queue'):
    global _events_queue
    _events_queue = queue


def _run_job(job_id, target):
    run_event = RunEvent(job_id)

    _events_queue.put(run_event.begin())
    try:
        target()
    except:
        _events_queue.put(run_event.error())
    else:
        _events_queue.put(run_event.done())


_events_queue = None
