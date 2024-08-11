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
from . import util
from .job.job import Run
from .event import RunEventer


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

        self._listeners = set()

    def run_job(self, *args, **kwargs) -> 'Run':
        job = self.add_job(*args, **kwargs)
        run = job.new_run()
        # TODO: other types of sched instead of just singleshot
        self._sched.run_singleshot(
            _run_job,
            kwargs = {
                'target': job.target,
                'job_id': run.job_id,
                'run_id': run.run_id,
                'prepare': job.mode == 'thread' and (
                    lambda: _prepare_thread_run(
                        self._th_queue, run.job_id, run.run_id,
                        module_logging_levels = self._sched.module_logging_levels,
                    )
                ) or None,
            },
            mode = job.mode,
        )
        return run

    def add_job(self, *args, **kwargs) -> 'Job':
        """
        Make a job and add to jober.
        """
        job = self.make_job(*args, **kwargs)
        self._id_to_job[job.id] = job
        return job

    def remove_job(self, job_id: str) -> bool:
        # TODO: more robust removable check
        job = self.get_job(job_id)
        if not job:
            logger.warning(f'remove_job: job ID not found {job_id}')
            return False
        if not job.removable:
            logger.warning(f'remove_job: job not removable {job_id}')
            return False
        del self._id_to_job[job_id]
        return True

    # TODO: mode should not be in Job
    # TODO: sched can be separated out from Job?
    def make_job(
            self,
            target: Union[str, Callable],
            args: tuple = (),
            kwargs: dict = {},
            *,
            name: str = None,
            extra: any = None,
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
        return make(
            target,
            name = name,
            extra = extra,
        )

    def start(self):
        self._sched.start()
        self._thread_events_thread.start()
        self._process_events_thread.start()
        util.enable_proxy()

    def stop(self):
        self._sched.stop()
        util.disable_proxy()

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

    def _make_thread_job(self, target: 'FuncTarget', *args, **kwargs) -> 'Job':
        from .job.thread_job import ThreadJob
        return ThreadJob(target, *args, **kwargs)

    def _make_process_job(self, target: 'ProcTarget', *args, **kwargs) -> 'Job':
        from .job.process_job import ProcessJob
        return ProcessJob(target, *args, **kwargs)

    def _collect_events(self, queue):
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

    default_mode = 'thread'
    n_threads = 4
    n_processes = 4


def _init_pool(queue: 'queue.Queue|multiprocessing.Queue'):
    global _events_queue
    _events_queue = queue


def _run_job(*, target, job_id, run_id, prepare):
    eventer = RunEventer(job_id = job_id, run_id = run_id)
    try:
        _events_queue.put(eventer.begin())
        if prepare:
            prepare()
        target()
    except:
        print(traceback.format_exc()) # output traceback in job run thread
        _events_queue.put(eventer.error())
    else:
        _events_queue.put(eventer.done())


def _prepare_thread_run(
        thread_out_queue, job_id, run_id,
        module_logging_levels = {},
):
    util.redirect(
        queue = thread_out_queue,
        job_id = job_id,
        run_id = run_id,
        module_logging_levels = module_logging_levels,
    )


_events_queue = None
