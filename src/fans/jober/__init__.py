"""
Instantiate <Jober>:

    jober = Jober()

Create <Job>:

    jober.run_job() -> Job
    jober.add_job() -> Job
    jober.make_job() -> Job

Schedule <Job>:

    job.schedule()

Query <Jober>:

    .get_jobs() -> List[Job] - get a list of known jobs
    .get_job(id) -> Job - get job by id

Query <Job>:

    .last_run: Run - Get last run instance of this job
    .runs: List[Run] - Get a list of known runs of this job

    # following attributes delegate to job's last run
    .status
    .finished
    .async_iter_output -> ...

Query <Run>:

    .status: str - Current status of job's one run
        'ready'     - not run yet
        'running'   - is running
        'done'      - finished with success
        'error'     - finished with error

    .finished: bool - Whether job run finished (with success or error)

    .async_iter_output -> ... - Get an async generator to iter job's output

    .get_output() -> str - Get job's output

Example jobs:

    - ocal (quick-console, switcha, stome)
    - fme (quantix.pricer, enos.backup)
    - stome (thumbnail.generate:ffmpeg, search)
"""
import time
import uuid
import shlex
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
        if target.type == Target.type_command:
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


class Target:

    type_command = 'command'
    type_python_callable = 'python_callable'
    type_python_script_callable = 'python_script_callable'
    type_python_module_callable = 'python_module_callable'
    type_python_script = 'python_script'
    type_python_module = 'python_module'

    @classmethod
    def make(
            cls,
            source: Union[Callable, str, List[str]],
            args = (),
            kwargs  = {},
    ):
        if callable(source):
            target_type = cls.type_python_callable
        elif isinstance(source, str):
            parts = shlex.split(source)
            if not parts:
                raise ValueError(f'invalid source "{source}"')
            target_type = cls.type_command
            if len(parts) == 1:
                if ':' in source:
                    domain_str, func_str = source.split(':')
                    if domain_str.endswith('.py'):
                        target_type = cls.type_python_script_callable
                    else:
                        target_type = cls.type_python_module_callable
                elif source.endswith('.py'):
                    target_type = cls.type_python_script
                elif '.' in source:
                    target_type = cls.type_python_module
        elif isinstance(source, list):
            target_type = 'command'
        else:
            raise ValueError(f'invalid source "{source}"')

        return cls(target_type, source, args, kwargs)

    def __init__(self, type, source, args, kwargs):
        self.type = type
        self.source = source
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        match self.type:
            case self.type_python_callable:
                self.source(*self.args, **self.kwargs)
            case self.type_python_script_callable:
                pass
            case self.type_python_module_callable:
                pass
            case self.type_python_script:
                pass
            case self.type_python_module:
                pass

    def _load_python_script_callable(self):
        import runpy

    def _load_python_module_callable(self):
        import importlib.util
        spec = importlib.util.find_spec(name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)


_events_queue = None
