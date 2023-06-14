import pytest

from fans.joberv2 import Jober


class Test_make_job:

    @classmethod
    def setup_class(cls):
        cls.jober = Jober()

    def test_job_has_id(self):
        job = self.jober.make_job(lambda: None)
        assert job.id

    def test_make_callable_job(self):
        job = self.jober.make_job(lambda: None)
        assert job.execution_mode == 'thread'
        assert job.source == f'[callable]{job.target.func}'

    def test_make_module_name_job(self):
        job = self.jober.make_job('foo.bar:func', mode = 'module')
        assert job.execution_mode == 'thread'
        assert job.source == '[module]foo.bar:func'

    def test_make_module_path_job(self):
        job = self.jober.make_job('/tmp/foo.py:func', mode = 'module path')
        assert job.execution_mode == 'thread'
        assert job.source == '[module]/tmp/foo.py:func'

    def test_make_python_script_job(self):
        job = self.jober.make_job('/tmp/foo.py', mode = 'py')
        assert job.execution_mode == 'process'
        assert job.source == '[script]/tmp/foo.py'

    def test_make_command_line_job(self):
        job = self.jober.make_job('/tmp/foo.py')
        assert job.execution_mode == 'process'
        assert job.source == '[command]/tmp/foo.py'

    def test_make_job_error_cases(self):
        with pytest.raises(ValueError) as e:
            self.jober.make_job(None)
            assert str(e).startswith('invalid job target')

        with pytest.raises(ValueError) as e:
            self.jober.make_job('', 'asdf')
            assert str(e).startswith('invalid job mode')


class Test_jober:

    @classmethod
    def setup_class(cls):
        cls.jober = Jober()

    def test_can_list_jobs(self):
        self.jober.make_job('ls')
        self.jober.make_job('date')
        assert len(self.jober.jobs) == 2

    def test_start_stop(self):
        self.jober.make_job('ls')
        self.jober.start()
        self.jober.stop()
