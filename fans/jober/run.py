import enum
import time
import queue
import inspect
import asyncio
import traceback
from typing import Callable, Optional

from fans.logger import get_logger
from fans.fn import noop

from fans.jober.target import Target
from fans.jober.capture import Capture


logger = get_logger(__name__)


class Run:
    
    class Status(enum.StrEnum):
        
        init = 'init'
        running = 'running'
        done = 'done'
        error = 'error'

    def __init__(
        self,
        target,
        *,
        job_id=None,
        run_id=None,
        args=None,
        kwargs=None,
        stdout: str = ':memory:',
        stderr: str = ':stdout:',
        on_event=noop,
    ):
        if args is not None or kwargs is not None:
            self.target = target.clone(args=args, kwargs=kwargs)
        else:
            self.target = target

        self.job_id = job_id
        self.run_id = run_id
        self.args = args
        self.kwargs = kwargs
        self.on_event = on_event

        self.status = Run.Status.init
        self.beg_time = None
        self.end_time = None
        self.trace = None
        self.result = None
        self.native_id = None  # apscheduler job id

        self._before_run = noop
        self.capture = Capture(stdout=stdout, stderr=stderr, should_enable_disable=False)
    
    def __call__(self):
        try:
            self._set_status(Run.Status.running)

            self.target.capture = self.capture

            # call target
            ret = self.target()

            # collect result
            if inspect.isgenerator(ret):
                self.result = list(ret)
            else:
                self.result = ret
            
            self._set_status(Run.Status.done)

            return ret
        except:
            self._set_status(Run.Status.error)

    @property
    def output(self) -> str:
        return self.capture.out_str

    @property
    def finished(self):
        return self.status in FINISHED_STATUSES
    
    def wait(self, interval=0.01):
        while self.status in RUNNING_STATUSES:
            time.sleep(interval)
    
    def as_dict(self):
        ret = {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'status': self.status,
            'beg_time': self.beg_time,
            'end_time': self.end_time,
        }
        return ret
    
    def _set_status(self, status):
        if status == Run.Status.running:
            self.beg_time = time.time()
        elif status in FINISHED_STATUSES:
            if status == Run.Status.error:
                self.trace = traceback.format_exc()
            self.end_time = time.time()

        self.status = status
        
        self.on_event({
            'type': 'run_status',
            'status': status,
            'time': time.time(),
            'job_id': self.job_id,
            'run_id': self.run_id,
        })


class DummyRun(Run):

    def __init__(self, job_id='dummy', run_id='dummy'):
        target = Target.make(noop)
        super().__init__(target=target, job_id=job_id, run_id=run_id)

    def __bool__(self):
        return False


class _Output:
    
    def __init__(self, run_eventer):
        self.run_eventer = run_eventer

    def write(self, string):
        if self.run_eventer:
            self.run_eventer.output(string)


dummy_run = DummyRun()

RUNNING_STATUSES = {Run.Status.init, Run.Status.running}
FINISHED_STATUSES = {Run.Status.done, Run.Status.error}
