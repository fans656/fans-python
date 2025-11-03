import time

import yaml
import pytest
from fans.path import Path
from fans.bunch import bunch

from fans.jober.jober import Jober
from fans.jober.tests.conftest import parametrized


class Test_load_jobs_from_conf:

    def test_load_jobs_from_conf(self, tmp_path):
        conf_path = tmp_path / 'conf.yaml'
        with conf_path.open('w') as f:
            yaml.dump({
                'jobs': [{
                    'name': 'foo',
                    'module': 'fans.jober.tests.samples.echo',
                }],
            }, f)
        
        jober = Jober(conf_path=conf_path)
        jobs = list(jober.jobs)
        assert jobs
        
        job = jober.get_job('foo')
        jober.run_job(job, args=['hello'], kwargs={'count': 2})
        job.wait()
        assert job.output == 'hello\nhello\n'


class Test_make_job:

    def test_job_has_id(self, jober):
        job = jober.make_job(lambda: None)
        assert job.id


class Test_get_job:

    def test_not_found(self, jober):
        assert jober.get_job('asdf') is None

    def test_found(self, jober):
        job = jober.add_job('ls')
        assert jober.get_job(job.id)

    @parametrized()
    def test_custom_id(self, conf, jober):
        jober.add_job(conf.target, id='foo')
        assert jober.get_job('foo').id == 'foo'


class Test_get_jobs:

    def test_get_jobs(self, jober):
        jober.add_job('ls')
        jober.add_job('date')
        jobs = list(jober.jobs)
        assert len(jobs) == 2


class Test_remove:

    def test_remove(self, jober):
        job = jober.add_job('ls')
        assert jober.get_job(job.id)
        assert jober.remove_job(job.id)
        assert jober.get_job(job.id) is None

    def test_not_removable_when_running(self, jober):
        job = jober.run_job('sleep 0.1')
        time.sleep(0.01)
        assert not jober.remove_job(job.id)
        job.wait()
        assert jober.remove_job(job.id)
        assert not jober.get_job(job.id)


class Test_run_status:

    def test_done(self, jober, mocker):
        job = jober.run_job(mocker.Mock())
        job.wait()
        assert job.status == 'done'

    def test_error(self, jober, mocker):
        job = jober.run_job(mocker.Mock(side_effect=Exception()))
        job.wait()
        assert job.status == 'error'


def test_conf_level_capture(tmp_path):
    
    def func():
        print('foo')

    with Jober(capture='default', root=tmp_path) as jober:
        job = jober.run_job(func)
        job.wait()
        assert job.last_run.capture.out_str == 'foo\n'

    with Jober(capture='file', root=tmp_path) as jober:
        job = jober.run_job(func)
        job.wait()
        assert job.last_run.capture.out_path.open().read() == 'foo\n'
