import traceback
import multiprocessing
from pathlib import Path
from enum import Enum
from typing import Union, Callable

from fans.bunch import bunch


class TargetType:

    module = 'module'
    module_path = 'module path'
    py = 'py'
    command = 'command'


class ExecutionMode:

    thread = 'thread'
    proc = 'proc'


class Jober:

    def __init__(self, *args, **kwargs):
        """
        root - root directory
        conf - {
            'root': str - root directory
        }
        """
        conf = make_conf(*args, **kwargs)

        self.root = conf.root
        self.n_threads = conf.n_threads

        self.id_to_job = {}
        self._thread_pool = None

    def add_job(self, *args, **kwargs) -> 'Job':
        """
        Make a job and add to jober.
        """
        job = self.make_job(*args, **kwargs)
        self.id_to_job[job.id] = job
        return job

    def make_job(
            self,
            target: Union[str, Callable],
            args: tuple = None,
            kwargs: dict = None,
            *,
            type: TargetType = TargetType.command,
            mode: ExecutionMode = None,
    ) -> 'Job':
        """
        Make a job without adding to jober.

        target: Union[str, Callable]
        args: tuple = None
        kwargs: dict = None
        type: str = 'command'
        mode: str = None

        Following type of target supported:

            Callable                    - thread/mp-process job
            module name / 'module'      - thread/mp-process job
            module path / 'module path' - thread/mp-process job
            script path / "py"          - process job
            command line                - process job
        """
        if isinstance(target, str):
            match type:
                case TargetType.module:
                    target = FuncTarget(
                        target, args = args, kwargs = kwargs, module_name = target)
                    make = self._make_thread_job
                case TargetType.module_path:
                    target = FuncTarget(
                        target, args = args, kwargs = kwargs, module_path = target)
                    make = self._make_thread_job
                case TargetType.py:
                    target = ProcTarget(target, path = target)
                    make = self._make_process_job
                case TargetType.command:
                    target = ProcTarget(target, cmd = target)
                    make = self._make_process_job
                case _:
                    raise ValueError(f'invalid job target type: "{type}"')
        elif callable(target):
            target = FuncTarget(func = target, args = args, kwargs = kwargs)
            make = self._get_job_maker_by_mode(mode)
        else:
            raise ValueError(f'invalid job target "{target}"')

        return make(target)

    def start(self):
        if self._has_thread_job:
            self._thread_pool = multiprocessing.ThreadPool(self.n_threads)

    def stop(self):
        if self._thread_pool:
            self._thread_pool.close()

    @property
    def jobs(self) -> 'Job':
        return list(self.iter_jobs)

    @property
    def iter_jobs(self):
        return self.id_to_job.values()

    @property
    def _has_thread_job(self):
        return any(job.execution_mode == 'thread' for job in self.iter_jobs)

    def _get_job_maker_by_mode(self, mode: ExecutionMode):
        match mode:
            case ExecutionMode.proc:
                return self._make_process_job
            case ExecutionMode.thread:
                return self._make_thread_job
            case _:
                return self._make_thread_job

    def _make_thread_job(self, target: 'FuncTarget') -> 'Job':
        from .job.thread_job import ThreadJob
        return ThreadJob(target)

    def _make_process_job(self, target: 'ProcTarget') -> 'Job':
        from .job.process_job import ProcessJob
        return ProcessJob(target)


class FuncTarget:

    def __init__(
            self,
            spec = None,
            args = None,
            kwargs = None,
            module_name = None,
            module_path = None,
            func = None,
    ):
        self.spec = spec
        self.module_name = module_name
        self.module_path = module_path
        self.module = None
        self.func = func
        self.args = args or tuple()
        self.kwargs = kwargs or dict()

        self.type = 'callable'

        if module_name or module_path:
            self.source = f'[module]{spec}'
        elif func:
            self.source = f'[callable]{func}'
        else:
            raise ValueError(f'invalid FuncTarget spec: "{spec}"')

    def __call__(self):
        if not self.func:
            pass # TODO: load module and set self.func
        try:
            self.func(*self.args, **self.kwargs)
        except:
            traceback.print_exc()


class ProcTarget:

    def __init__(
            self,
            spec = None,
            path = None,
            cmd = None,
    ):
        self.spec = spec
        self.path = path
        self.cmd = cmd

        if path:
            self.source = f'[script]{path}'
            self.type = 'script'
        elif cmd:
            self.source = f'[command]{cmd}'
            self.type = 'command'
        else:
            raise ValueError(f'invalid ProcTarget spec: "{spec}"')


def make_conf(
        root: str = None,
        conf: dict = None,
        conf_path: Path = None,
):
    return bunch(
        root = None,
        n_threads = 4,
    )
