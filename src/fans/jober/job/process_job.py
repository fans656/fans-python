import multiprocessing as mp

from .job import Job


class ProcessJob(Job):

    mode = 'process'

    def init(self):
        self.proc = None

    def _make_runnable(self):
        return _run_job, {
            'id': self.id,
            'target': self.target,
        }


class Runnable:

    def __init__(self):
        pass

    def __call__(self):
        pass


def _run_job(target, **_):
    target()
