import time
from pathlib import Path

from fans.jober import Jober


def test_run_script_by_absolute_path(jober):
    script_path = Path(__file__).parent / 'samples/echo.py'
    job = jober.run_job(str(script_path), args=('foo',))
    job.wait()
    assert job.output == 'foo\n'


def test_max_recent_runs():
    jober = Jober(max_recent_runs=2)
    job = jober.add_job('date', when=0.02)
    time.sleep(0.2)
    jober.stop()
    assert len(job.runs) == 2


def test_generator_func(jober):

    def func():
        for i in range(5):
            yield i + 1

    job = jober.run_job(func)
    job.wait()
    assert job.last_run.result == [1, 2, 3, 4, 5]
