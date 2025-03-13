import time

import yaml
import pytest
from fans.path import Path

from fans.jober.jober import Jober


class Test_make_job:

    def test_job_has_id(self, jober):
        job = jober.make_job(lambda: None)
        assert job.id

    def test_make_callable_job(self, jober):
        job = jober.make_job(lambda: None)
        assert job.mode == default_conf.default_mode

    def test_make_callable_proc_job(self, jober):
        job = jober.make_job(lambda: None, mode = 'process')
        assert job.mode == 'process'

    def test_make_module_name_job(self, jober):
        job = jober.make_job('foo.bar:func')
        assert job.mode == default_conf.default_mode

    def test_make_module_path_job(self, jober):
        job = jober.make_job('/tmp/foo.py:func')
        assert job.mode == default_conf.default_mode

    def test_make_python_script_job(self, jober):
        job = jober.make_job('/tmp/foo.py')
        assert job.mode == default_conf.default_mode

    def test_make_command_line_job(self, jober):
        job = jober.make_job('ls -lh')
        assert job.mode == 'process'

    def test_make_job_error_cases(self, jober):
        with pytest.raises(ValueError) as e:
            jober.make_job(None)
            assert str(e).startswith('invalid job target')

        with pytest.raises(ValueError) as e:
            jober.make_job('', 'asdf')
            assert str(e).startswith('invalid job target type')


class Test_get:

    def test_initial(self, jober):
        assert jober.get_job('asdf') is None

    def test_get(self, jober):
        job = jober.add_job('ls')
        assert jober.get_job(job.id)

    def test_get_jobs(self, jober):
        jober.add_job('ls')
        jober.add_job('date')
        jobs = jober.get_jobs()
        assert len(jobs) == 2


class Test_remove:

    def test_remove(self, jober):
        job = jober.add_job('ls')
        assert jober.get_job(job.id)
        assert jober.remove_job(job.id)
        assert jober.get_job(job.id) is None

    # TODO: test unremovable


class Test_jober:

    def test_start_stop(self, jober):
        jober.make_job('ls')
        jober.start()
        jober.stop()


# TODO: multiple target, multiple mode
class Test_runnable_job:

    def test_job_status_done(self):
        jober = Jober()
        job = jober.run_job(self.func, mode = 'process')
        assert job.status == 'init'

        jober.start()
        wait_when_status(job, 'init')
        assert job.status == 'running'

        wait_when_status(job, 'running')
        assert job.status == 'done'

        jober.stop()

    def test_job_status_error(self):
        jober = Jober()
        job = jober.run_job(self.func_error, mode = 'process')
        assert job.status == 'init'

        jober.start()
        wait_when_status(job, 'init')
        assert job.status == 'running'

        wait_when_status(job, 'running')
        assert job.status == 'error'

        jober.stop()

    def func(self):
        time.sleep(0.001)

    def func_error(self):
        time.sleep(0.001)
        raise Exception('oops')


class Test_process_job:

    pass


class Test_tracked_process_job:

    pass


def wait_when_status(target, status, timeout = 1):
    beg = time.time()
    while True:
        if target.status != status:
            break
        if time.time() - beg >= timeout:
            break
        time.sleep(0.001)
