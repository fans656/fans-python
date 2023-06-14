import multiprocessing
from pathlib import Path
from typing import Union, Callable

from fans.bunch import bunch


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

    def make_job(self, *args, **kwargs) -> 'Job':
        """
        target: Union[str, Callable]
        mode: str = 'command'

        Following type of target supported:

            Callable                    - thread job
            module name / 'module'      - thread job
            module path / 'module path' - thread job
            script path / "py"          - process job
            command line                - process job
        """
        job = self._make_job(*args, **kwargs)
        self.id_to_job[job.id] = job
        return job

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

    def _make_job(
            self,
            target: Union[str, Callable],
            mode: str = 'command',
    ):
        if isinstance(target, str):
            match mode:
                case 'module':
                    return self._make_thread_job(FuncTarget(target, module_name = target))
                case 'module path':
                    return self._make_thread_job(FuncTarget(target, module_path = target))
                case 'py':
                    return self._make_process_job(ProcTarget(target, path = target))
                case 'command':
                    return self._make_process_job(ProcTarget(target, cmd = target))
                case _:
                    raise ValueError(f'invalid job mode: {mode}')
        elif callable(target):
            return self._make_thread_job(FuncTarget(func = target, ready = True))
        else:
            raise ValueError(f'invalid job target {target}')

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
            module_name = None,
            module_path = None,
            func = None,
            ready = False,
    ):
        self.spec = spec
        self.module_name = module_name
        self.module_path = module_path
        self.module = None
        self.func = func
        self.ready = ready

        if module_name or module_path:
            self.source = f'[module]{spec}'
        elif func:
            self.source = f'[callable]{func}'
        else:
            raise ValueError(f'invalid FuncTarget spec: "{spec}"')

    def __call__(self):
        pass


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
        elif cmd:
            self.source = f'[command]{cmd}'
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
