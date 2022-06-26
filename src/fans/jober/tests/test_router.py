import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from fans.jober import errors
from fans.jober.router import app
from fans.jober.jober import Jober


def test_startup_shutdown(mocker):
    jober = Jober.get_instance()
    jober.start = mocker.Mock()
    jober.stop = mocker.Mock()

    with TestClient(app) as client:
        pass

    jober.start.assert_called_once()
    jober.stop.assert_called_once()
    Jober._instance = None


def test_show_jobs():
    with Client() as client:
        # initialy no jobs
        data = client.get('/api/job/show').json()
        assert data['jobs'] == []

        # make some jobs
        client.post('/api/job/make', json = {
            'name': 'foo',
        })

        # return the made jobs
        data = client.get('/api/job/show').json()
        jobs = data['jobs']
        assert len(jobs) == 1
        job = jobs[0]
        assert job['name'] == 'foo'


def test_show_job():
    with Client() as client:
        # not existed
        res = client.get('/api/job/show', params = {'name': 'foo'})
        assert res.status_code == 404

        client.post('/api/job/make', json = {'name': 'foo'})
        res = client.get('/api/job/show', params = {'name': 'foo'})
        assert res.status_code == 200
        assert res.json()['name'] == 'foo'


def test_add_job():
    with Client() as client:
        # make some jobs
        res = client.post('/api/job/make', json = {'name': 'foo'})
        assert res.status_code == 200
        # make with same name will fail
        res = client.post('/api/job/make', json = {'name': 'foo'})
        assert res.status_code == 409


def test_run_job():
    with Client() as client:
        res = client.get('/api/job/run', params = {'name': 'foo'})
        assert res.status_code == 404


class Client:

    def __enter__(self):
        self.client = TestClient(app)
        self.client.__enter__()
        return self.client

    def __exit__(self, *args, **kwargs):
        self.client.__exit__(*args, **kwargs)
        Jober._instance = None
