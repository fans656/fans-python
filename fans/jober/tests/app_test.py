import time
import contextlib

import yaml
import pytest
from starlette.testclient import TestClient
from fans.bunch import bunch
from fans.fn import noop

from fans.jober.app import root_app
from fans.jober.jober import Jober


@pytest.fixture
def client():
    yield TestClient(root_app)


@pytest.fixture
def jober():
    with use_instance() as jober:
        yield jober


@contextlib.contextmanager
def use_instance(conf: bunch = {}):
    jober = Jober(**conf)
    Jober._instance = jober
    jober.start()
    yield jober
    jober.stop()
    Jober._instance = None


class Test_list_jobs:

    def test_empty_jobs_by_default(self, client):
        assert client.get('/api/list-jobs').json()['data'] == []
    
    def test_list_jobs(self, jober, client):
        jober.add_job(noop)
        jober.add_job(noop)

        jobs = client.get('/api/list-jobs').json()['data']

        assert len(jobs) == 2
        for job in jobs:
            assert 'id' in job


class Test_get_job:
    
    def test_get_job(self, jober, client):
        job = jober.add_job(noop)

        data = client.get('/api/get-job', params={
            'job_id': job.id,
        }).json()

        assert data['id'] == job.id


class Test_list_runs:
    
    def test_list_runs(self, jober, client):
        job = jober.add_job(noop)

        jober.run_job(job).wait()
        jober.run_job(job).wait()

        runs = client.get('/api/list-runs', params={
            'job_id': job.id,
        }).json()['data']

        assert len(runs) == 2
        for run in runs:
            assert 'job_id' in run
            assert 'run_id' in run
            assert 'status' in run
            assert 'beg_time' in run
            assert 'end_time' in run


class Test_get_jober:

    def test_get_jober(self, client, tmp_path):
        conf_path = tmp_path / 'conf.yaml'
        with conf_path.open('w') as f:
            yaml.dump({}, f)

        with use_instance({'conf_path': conf_path}):
            data = client.get('/api/get-jober').json()
            
            # can get conf path
            assert data['conf_path'] == str(conf_path)


class Test_prune_jobs:
    
    def test_prune(self, jober, mocker, client):
        job = jober.run_job(noop)
        pruned_jobs = client.post('/api/prune-jobs').json()
        assert len(pruned_jobs) == 1
        assert pruned_jobs[0]['id'] == job.id


class Test_run_job:
    
    def test_simple(self, mocker, jober, client):
        func = mocker.Mock()
        job = jober.add_job(func)

        client.post('/api/run-job', json={'job_id': job.id})

        time.sleep(0.01)
        func.assert_called()
