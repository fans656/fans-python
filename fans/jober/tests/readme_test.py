import os
import time
import datetime
from pathlib import Path

from freezegun import freeze_time

from fans.jober import Jober
from fans.jober.tests.samples.echo import echo


def test_simple():
    Jober(capture=False).run_job('date').wait()


def test_periodical():
    Jober(capture=False).add_job(lambda: print(time.time()), when=1).wait()


def test_cron(jober, mocker):
    with freeze_time('2025-01-01 00:00:00+08:00'):
        jober = Jober()
        func = mocker.Mock()
        jober.add_job(func, when='0 22 * * *')

        with freeze_time('2025-01-01 21:59:00+08:00'):
            jober._sched._sched.wakeup()
            time.sleep(0.1)

        assert func.call_count == 0

        with freeze_time('2025-01-01 22:00:00+08:00'):
            jober._sched._sched.wakeup()
            time.sleep(0.1)

        assert func.call_count == 1

        with freeze_time('2025-01-02 22:00:00+08:00'):
            jober._sched._sched.wakeup()
            time.sleep(0.1)

        assert func.call_count == 2
        
        jober.stop()


def test_shell_command(jober):
    jober.run_job('sleep 0.01 && date', shell=True).wait()


def test_shell_command_without_shell_true(jober):
    jober.run_job('ls -lh').wait()


def test_shell_command_as_list(jober):
    dir_path = Path(__file__).parent.absolute()
    job = jober.run_job(['ls', '-lh'], cwd=dir_path)
    job.wait()
    assert 'readme_test.py' in job.output


def test_python_callable(jober):
    def func():
        print('hello')
    job = jober.run_job(func)
    job.wait()
    assert job.output == 'hello\n'


def test_python_module(jober):
    job = jober.run_job('fans.jober.tests.samples.echo', args=('hello',))
    job.wait()
    assert job.output == 'hello\n'


def test_python_module_callable(jober):
    job = jober.run_job('fans.jober.tests.samples.echo:say')
    job.wait()
    assert job.output == 'hi\n'


def test_python_script(jober):
    dir_path = Path(__file__).parent.absolute()
    job = jober.run_job('./samples/echo.py', args=('hello',), cwd=dir_path)
    job.wait()
    assert job.output == 'hello\n'


def test_python_script_callable(jober):
    dir_path = Path(__file__).parent.absolute()
    job = jober.run_job('./samples/echo.py:say', cwd=dir_path)
    job.wait()
    assert job.output == 'hi\n'


def test_run_callable_in_process(jober):
    job = jober.run_job(echo, kwargs={'show_pid': True}, process=True)
    job.wait()
    assert job.last_run
    print(job.last_run)
    print(repr(job.last_run._capture.out))
    assert int(job.output) != os.getpid()


def test_run_module_callable_in_process(jober):
    job = jober.run_job('fans.jober.tests.samples.echo:echo', kwargs={'show_pid': True}, process=True)
    job.wait()
    assert int(job.output) != os.getpid()


def test_run_script_callable_in_process(jober):
    dir_path = Path(__file__).parent.absolute()
    job = jober.run_job('./samples/echo.py:echo', cwd=dir_path, kwargs={'show_pid': True}, process=True)
    job.wait()
    assert int(job.output) != os.getpid()


def test_cwd(jober, tmp_path):
    job = jober.run_job('pwd', cwd=tmp_path)
    job.wait()
    assert job.output.strip() == str(tmp_path)


def test_interval():
    jober = Jober(max_recent_runs=100)
    job = jober.add_job('date', when=0.01)
    time.sleep(0.1)
    jober.stop()
    assert len(job.runs) > 9
    for run in job.runs:
        assert run.output
