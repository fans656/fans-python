import time

import yaml
import pytest
from fans.path import Path

from fans.jober.jober import Jober, DEFAULT_MODE
from fans.jober.conftest import parametrized


class Test_make_job:

    def test_job_has_id(self, jober):
        job = jober.make_job(lambda: None)
        assert job.id


class Test_get_job:
    """Can get job by ID"""

    def test_not_found(self, jober):
        assert jober.get_job('asdf') is None

    def test_found(self, jober):
        job = jober.add_job('ls')
        assert jober.get_job(job.id)

    @parametrized()
    def test_custom_id(self, conf, jober):
        jober.add_job(conf.target, id='foo')
        assert jober.get_job('foo')


class Test_get_jobs:
    """Can list all jobs"""

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

    def test_not_removable_when_running(self, jober):
        run = jober.run_job('sleep 0.1')
        assert not jober.remove_job(run.job_id)
        run.wait()
        assert jober.remove_job(run.job_id)
        assert not jober.get_job(run.job_id)


class Test_run_status:

    def test_done(self, jober, mocker):
        run = jober.run_job(mocker.Mock())
        run.wait()
        assert run.status == 'done'

    def test_error(self, jober, mocker):
        run = jober.run_job(mocker.Mock(side_effect=Exception()))
        run.wait()
        assert run.status == 'error'
