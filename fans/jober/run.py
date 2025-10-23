import time
import queue
import inspect
import asyncio
import traceback
from typing import Callable, Optional

from fans.logger import get_logger
from fans.fn import noop

from fans.jober.event import EventType, RunEventer
from fans.jober.target import Target
from fans.jober.capture import Capture


logger = get_logger(__name__)


class Run:

    def __init__(
        self,
        *,
        target,
        job_id,
        run_id,
        args=None,
        kwargs=None,
        stdout: str = ':memory:',
        stderr: str = ':stdout:',
    ):
        if args is not None or kwargs is not None:
            self.target = target.clone(args=args, kwargs=kwargs)
        else:
            self.target = target

        self.job_id = job_id
        self.run_id = run_id
        self.args = args
        self.kwargs = kwargs

        self.status = 'init'
        self.beg_time = None
        self.end_time = None
        self.trace = None
        self.result = None
        self.native_id = None  # apscheduler job id

        self.get_events_queue = noop

        self._before_run = noop
        self._capture = Capture(stdout=stdout, stderr=stderr, should_enable_disable=False)
    
    def __call__(self):
        eventer = RunEventer(job_id=self.job_id, run_id=self.run_id, queue=self.get_events_queue())
        try:
            eventer.begin()
            self.beg_time = time.time()

            self.target.capture = self._capture
            ret = self.target()

            if inspect.isgenerator(ret):
                self.result = list(ret)
            else:
                self.result = ret
            
            eventer.done()

            return ret
        except:
            print(traceback.format_exc()) # output traceback in job run thread
            eventer.error()
        finally:
            self.end_time = time.time()

    @property
    def output(self) -> str:
        return self._capture.out

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

    def _on_run_event(self, event):
        match event['type']:
            case EventType.job_run_begin:
                self.status = 'running'
            case EventType.job_run_done:
                self.status = 'done'
            case EventType.job_run_error:
                self.status = 'error'
                self.trace = event.get('trace')
            case _:
                logger.warning(f'invalid event: {event}')


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

RUNNING_STATUSES = {'init', 'running'}
FINISHED_STATUSES = {'done', 'error'}
