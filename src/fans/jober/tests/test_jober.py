import time

import yaml
import pytest
from fans.path import Path

from fans.jober import Jober, make_conf, conf_default


class Test_make_job:

    @classmethod
    def setup_class(cls):
        cls.jober = Jober()

    def test_job_has_id(self):
        job = self.jober.make_job(lambda: None)
        assert job.id

    def test_make_callable_job(self):
        job = self.jober.make_job(lambda: None)
        assert job.mode == 'thread'
        assert job.source == f'[callable]{job.target.func}'

    def test_make_callable_proc_job(self):
        job = self.jober.make_job(lambda: None, mode = 'process')
        assert job.mode == 'process'
        assert job.source == f'[callable]{job.target.func}'

    def test_make_module_name_job(self):
        job = self.jober.make_job('foo.bar:func', type = 'module')
        assert job.mode == 'thread'
        assert job.source == '[module]foo.bar:func'

    def test_make_module_path_job(self):
        job = self.jober.make_job('/tmp/foo.py:func', type = 'module path')
        assert job.mode == 'thread'
        assert job.source == '[module]/tmp/foo.py:func'

    def test_make_python_script_job(self):
        job = self.jober.make_job('/tmp/foo.py', type = 'py')
        assert job.mode == 'process'
        assert job.source == '[script]/tmp/foo.py'

    def test_make_command_line_job(self):
        job = self.jober.make_job('/tmp/foo.py')
        assert job.mode == 'process'
        assert job.source == '[command]/tmp/foo.py'

    def test_make_job_error_cases(self):
        with pytest.raises(ValueError) as e:
            self.jober.make_job(None)
            assert str(e).startswith('invalid job target')

        with pytest.raises(ValueError) as e:
            self.jober.make_job('', 'asdf')
            assert str(e).startswith('invalid job target type')


class Test_jober:

    @classmethod
    def setup_class(cls):
        cls.jober = Jober()

    def test_can_list_jobs(self):
        self.jober.add_job('ls')
        self.jober.add_job('date')
        assert len(self.jober.get_jobs()) == 2

    def test_start_stop(self):
        self.jober.make_job('ls')
        self.jober.start()
        self.jober.stop()


class Test_make_conf:

    def test_warning_when_error_reading_conf_path(self, tmpdir, caplog):
        conf_path = Path(tmpdir) / 'conf.yaml'
        with conf_path.open('w') as f:
            f.write('asdf')
        conf = make_conf(conf_path)
        assert any(d.message.startswith('error reading conf from') for d in caplog.records)

    def test_read_conf_path(self, tmpdir):
        conf_path = Path(tmpdir) / 'conf.yaml'
        sample_conf = {
            'root': '/tmp/foo',
            'n_threads': 8,
            'n_processes': 8,
        }
        with conf_path.open('w') as f:
            yaml.dump(sample_conf, f)

        conf = make_conf(conf_path)
        for key, value in sample_conf.items():
            assert conf[key] == value

    def test_conf_defaults(self):
        conf = make_conf()
        for key in [d for d in dir(conf_default) if not d.startswith('_')]:
            value = getattr(conf_default, key)
            assert conf[key] == value


class Test_runnable_job:

    def test_job_status_done(self):
        jober = Jober()
        job = jober.run_job(self.func, mode = 'process')
        assert job.status == 'ready'

        jober.start()
        wait_when_status(job, 'ready')
        assert job.status == 'running'

        wait_when_status(job, 'running')
        assert job.status == 'done'

        jober.stop()

    def test_job_status_error(self):
        jober = Jober()
        job = jober.run_job(self.func_error, mode = 'process')
        assert job.status == 'ready'

        jober.start()
        wait_when_status(job, 'ready')
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
