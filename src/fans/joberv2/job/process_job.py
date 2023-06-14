import multiprocessing as mp

from .job import Job


class ProcessJob(Job):

    execution_mode = 'process'

    def init(self):
        self.proc = None

    def start(self):
        match self.target.type:
            case 'callable':
                self._start_callable_proc()
            case _:
                pass

    def join(self):
        self.proc.join()

    def _start_callable_proc(self):
        self.proc = mp.Process(target = self.target)
        self.proc.start()
