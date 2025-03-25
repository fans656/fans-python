import datetime
from pathlib import Path

from fans.jober import Jober


def test_simple():
    jober = Jober(capture=False)
    jober.run_job('sleep 0.01 && date', shell=True).wait()
    jober.stop()


def test_periodical():
    def func():
        print(datetime.datetime.now())
    jober = Jober(capture=False)
    jober.add_job(func, sched=0.01)
    jober.wait(0.1)
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
