import time
import traceback


class EventType:

    job_run_begin = 'job_run_begin'
    job_run_done = 'job_run_done'
    job_run_error = 'job_run_error'
    job_run_output = 'job_run_output'


class RunEventer:

    def __init__(self, *, job_id, run_id):
        self.job_id = job_id
        self.run_id = run_id

    def begin(self):
        return self._event(EventType.job_run_begin)

    def done(self):
        return self._event(EventType.job_run_done)

    def error(self):
        return self._event(EventType.job_run_error, trace = traceback.format_exc())

    def output(self, content):
        return self._event(EventType.job_run_output, content = content)

    def _event(self, event_type, **data):
        return {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'type': event_type,
            'time': time.time(),
            **data,
        }
