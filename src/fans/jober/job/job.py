import uuid
import queue
import asyncio
from abc import abstractmethod


class Job:
    """
    Base impl, see separate concrete impl for details.
    """

    mode = None

    def __init__(self, target: any):
        self.target = target
        self.id = uuid.uuid4().hex

        self._id_to_run = {}
        self._last_run_id = None
        self._max_run_time = 0

        # TODO: limit output size
        self._output_queue = queue.Queue()

        self.init()

    @abstractmethod
    def init(self):
        pass

    @property
    def status(self) -> str:
        return self.last_run.status

    @property
    def trace(self) -> str:
        return self.last_run.trace

    @property
    def finished(self):
        return self.last_run.status in finished_statuses

    @property
    def last_run(self):
        return self._id_to_run.get(self._last_run_id) or dummy_run

    @property
    def source(self) -> str:
        return self.target.source

    # TODO: delegate to run
    def iter_output(self, timeout = None):
        cur = ''
        while True:
            out = self._output_queue.get()
            if not out:
                break
            cur += out
            if cur.endswith('\n'):
                yield cur[:-1]
                cur = ''

    # TODO: delegate to run
    async def iter_output_async(self, timeout = None):
        cur = ''
        while True:
            try:
                out = self._output_queue.get(False)
            except queue.Empty:
                await asyncio.sleep(0.001)
            else:
                if not out:
                    break
                cur += out
                if cur.endswith('\n'):
                    yield cur[:-1]
                    cur = ''

    def _on_run_event(self, event):
        run_id = event['run_id']

        if event['type'] == 'output':
            self._output_queue.put(event['content'])
            return

        if event['time'] > self._max_run_time:
            self._last_run_id = run_id

        if run_id not in self._id_to_run:
            self._id_to_run[run_id] = Run(run_id)
        run = self._id_to_run[run_id]

        match event['type']:
            case 'job_run_begin':
                run.status = 'running'
            case 'job_run_done':
                run.status = 'done'
                self._output_queue.put(None)
            case 'job_run_error':
                run.status = 'error'
                run.trace = event.get('trace')
                self._output_queue.put(None)


class Run:

    def __init__(self, run_id):
        self.id = run_id
        self.status = 'ready'
        self.trace = None


class DummyRun(Run):

    def __init__(self):
        super().__init__('dummy')

    def __bool__(self):
        return False


dummy_run = DummyRun()
finished_statuses = {'done', 'error'}
