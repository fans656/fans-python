import pytest

from fans.jober.event import EventType
from ..run import Run


class Test_run:

    async def test_last_output_without_newline_can_be_collected(self):
        run = Run(job_id = None, run_id = None)
        run._on_run_event({'type': EventType.job_run_begin})
        run._on_run_event({'type': EventType.job_run_output, 'content': 'hello'})
        run._on_run_event({'type': EventType.job_run_output, 'content': '\n'})
        run._on_run_event({'type': EventType.job_run_output, 'content': 'world'})
        run._on_run_event({'type': EventType.job_run_done})
        
        done = False
        events = []
        async for event in run.iter_events_async():
            match event['type']:
                case EventType.job_run_output:
                    events.append(event)
                case EventType.job_run_done:
                    done = True
        assert events[-1]['content'] == 'world'
