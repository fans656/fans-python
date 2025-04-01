from pathlib import Path


def test_run_script_by_absolute_path(jober):
    script_path = Path(__file__).parent / 'samples/echo.py'
    job = jober.run_job(str(script_path), args=('foo',))
    job.wait()
    assert job.output == 'foo\n'
