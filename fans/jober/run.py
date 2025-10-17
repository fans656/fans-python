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
        self.trace = None
        self._outputs = []
        self.result = None

        self.ctime = time.time()

        # TODO: limit output size
        # NOTE: this does not support multiple clients
        self._events_queue = queue.Queue()
    
    def __call__(self, *, events_queue, before_run=noop):
        eventer = RunEventer(job_id=self.job_id, run_id=self.run_id, queue=events_queue)
        try:
            eventer.begin()

            before_run()

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

    @property
    def output(self) -> str:
        return ''.join(self._outputs)

    @property
    def output_lines(self) -> list[str]:
        # TODO: find newline upon append instead of here
        return self.output.rstrip('\n').split('\n')

    @property
    def finished(self):
        return self.status in finished_statuses
    
    def wait(self, interval=0.01):
        while self.status in ('init', 'running'):
            time.sleep(interval)

    async def iter_events_async(self, should_stop: Optional[Callable[[], bool]] = None):
        content_event = None
        content = ''
        async for event in self._iter_events_async(should_stop):
            if event['type'] == EventType.job_run_output:
                content += event['content']
                content_event = event
                if content.endswith('\n'):
                    yield {**content_event, 'content': content}
                    content = ''
                    content_event = None
            else:
                if content and event['type'] in finished_event_types:
                    yield {**content_event, 'content': content}
                yield event

    async def _iter_events_async(self, should_stop: Optional[Callable[[], bool]] = None):
        while True:
            try:
                event = self._events_queue.get(False)
            except queue.Empty:
                if should_stop and await should_stop():
                    break
                await asyncio.sleep(0.001)
            else:
                yield event
                if event['type'] in finished_event_types:
                    break

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
        self._events_queue.put(event)


class DummyRun(Run):

    def __init__(self, job_id='dummy', run_id='dummy'):
        target = Target.make(noop)
        super().__init__(target=target, job_id=job_id, run_id=run_id)

    def __bool__(self):
        return False


dummy_run = DummyRun()
finished_statuses = {'done', 'error'}
finished_event_types = {EventType.job_run_done, EventType.job_run_error}
