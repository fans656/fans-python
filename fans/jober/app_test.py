import contextlib

import yaml
import pytest
from starlette.testclient import TestClient
from fans.bunch import bunch

from .app import app
from .jober import Jober


@pytest.fixture
def client():
    yield TestClient(app)


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


class Test_info:

    def test_jober_info(self, client, tmp_path):
        """Can get general info about jober"""
        conf_path = tmp_path / 'conf.yaml'
        with conf_path.open('w') as f:
            yaml.dump({}, f)

        with use_instance({'conf_path': conf_path}):
            data = client.get('/api/jober/info').json()
            
            # can get conf path
            assert data['conf_path'] == str(conf_path)
    
    def test_job_info(self, jober, mocker, client):
        """Can get info about specific job"""
        job = jober.add_job(mocker.Mock())
        data = client.get('/api/jober/info', params={'job_id': job.id}).json()
        assert data['id'] == job.id
    
    #def test_run_info(self, jober, mocker, client):
    #    """Can get info about specific run"""
    #    job = jober.add_job(mocker.Mock())
    #    client.post('/api/jober/run', json={'job_id': job.id})
    #    #job.wait()
    #    #data = client.get('/api/jober/info', params={'job_id': job.id}).json()
    #    #assert data['id'] == job.id


class Test_list:

    def test_empty(self, client):
        """By default there is no jobs"""
        assert client.get('/api/jober/list').json() == []
    
    def test_non_empty(self, jober, mocker, client):
        """Can list existed jobs"""
        jober.add_job(mocker.Mock())
        jober.add_job(mocker.Mock())
        jobs = client.get('/api/jober/list').json()
        assert len(jobs) == 2
        for job in jobs:
            assert 'id' in job


class Test_prune:
    
    def test_prune(self, jober, mocker, client):
        """Can prune jobs not running"""
        jober.add_job(mocker.Mock())
        pruned_jobs = client.post('/api/jober/prune').json()
        assert pruned_jobs
        for job in pruned_jobs:
            assert 'id' in job


class Test_run:
    
    def test_run(self, jober, mocker, client):
        """Can manually trigger run of specific job"""
        func = mocker.Mock()
        job = jober.add_job(func)
        client.post('/api/jober/run', json={'job_id': job.id})
        import time
        time.sleep(0.1)
        func.assert_called()
