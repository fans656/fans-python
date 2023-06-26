import queue
import asyncio

from fans.logger import get_logger

from fans.jober.event import EventType


logger = get_logger(__name__)


class Run:

    def __init__(self, *, job_id, run_id):
        self.job_id = job_id
        self.run_id = run_id
        self.status = 'ready'
        self.trace = None

        # TODO: limit output size
        # NOTE: this does not support multiple clients
        self._events_queue = queue.Queue()

    async def iter_events_async(self, should_stop = None):
        content_event = None
        content = ''
        async for event in self._iter_events_async(should_stop = should_stop):
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

    async def _iter_events_async(self, should_stop = None):
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
                pass
            case _:
                logger.warning(f'invalid event: {event}')
        self._events_queue.put(event)


class DummyRun(Run):

    def __init__(self):
        super().__init__(job_id = 'dummy', run_id = 'dummy')

    def __bool__(self):
        return False


dummy_run = DummyRun()
finished_statuses = {'done', 'error'}
finished_event_types = {EventType.job_run_done, EventType.job_run_error}
