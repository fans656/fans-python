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


def test_disable(jober):
    job = jober.add_job('date', when=0.01, max_recent_runs=999)
    time.sleep(0.03)

    job.disable()
    time.sleep(0.03)

    job.enable()
    time.sleep(0.03)

    jober.stop()
    assert abs(len(job.runs) - 6) < 2


def test_run_existing_job_with_modified_args(jober):
    job = jober.run_job('fans.jober.tests.samples.echo', args=('hello',))
    job.wait()
    assert job.output == 'hello\n'

    jober.run_job(job, args=('foo',), kwargs={'count': 3})
    time.sleep(0.01)
    job.wait()
    assert job.output == 'foo\nfoo\nfoo\n'
