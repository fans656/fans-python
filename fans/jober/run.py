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


logger = get_logger(__name__)


class Run:

    def __init__(self, *, target, job_id, run_id, args=(), kwargs={}):
        if args or kwargs:
            self.target = target.bind(args, kwargs)
        else:
            self.target = target
        self.job_id = job_id
        self.run_id = run_id
        self.args = args
        self.kwargs = kwargs

        self.status = 'init'
        self.ctime = time.time()
        self.trace = None
        self.result = None

        self._outputs = []
    
    def __call__(self, *, events_queue, before_run=noop):
        eventer = RunEventer(job_id=self.job_id, run_id=self.run_id, queue=events_queue)
        try:
            eventer.begin()

            # before run
            before_run()

            # run
            ret = self.target()

            # after run
            if inspect.isgenerator(ret):
                self.result = list(ret)
            else:
                self.result = ret
            
            eventer.done()

            return ret
        except:
            print(traceback.format_exc()) # output traceback in job run thread
            eventer.error()

    @property
    def output(self) -> str:
        return ''.join(self._outputs)

    @property
    def finished(self):
        return self.status in FINISHED_STATUSES
    
    def wait(self, interval=0.01):
        while self.status in RUNNING_STATUSES:
            time.sleep(interval)

    def _on_run_event(self, event):
        match event['type']:
            case EventType.job_run_begin:
                self.status = 'running'
            case EventType.job_run_done:
                self.status = 'done'
            case EventType.job_run_error:
                self.status = 'error'
                self.trace = event.get('trace')
            case EventType.job_run_output:
                self._outputs.append(event['content'])
            case _:
                logger.warning(f'invalid event: {event}')


class DummyRun(Run):

    def __init__(self, job_id='dummy', run_id='dummy'):
        target = Target.make(noop)
        super().__init__(target=target, job_id=job_id, run_id=run_id)

    def __bool__(self):
        return False


dummy_run = DummyRun()

RUNNING_STATUSES = {'init', 'running'}
FINISHED_STATUSES = {'done', 'error'}
