import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from fans.jober import errors
from fans.jober.app import app
from fans.jober.jober import Jober


def test_startup_shutdown(mocker):
    jober = Jober.get_instance()
    jober.start = mocker.Mock()
    jober.stop = mocker.Mock()

    with Client() as client:
        pass

    # Jober.start will be called upon app start
    jober.start.assert_called_once()
    # Jober.stop will be called upon app shutdown
    jober.stop.assert_called_once()

    Jober._instance = None


def test_startup_shutdown_using_sub_app(mocker):
    jober = Jober.get_instance()
    jober.start = mocker.Mock()
    jober.stop = mocker.Mock()

    root_app = FastAPI()
    root_app.mount('/', app)
    with TestClient(root_app):
        pass
    assert not jober.start.called
    assert not jober.stop.called


    @root_app.on_event('startup')
    def on_startup():
        app.state.startup()

    @root_app.on_event('shutdown')
    def on_shutdown():
        app.state.shutdown()

    with TestClient(root_app):
        pass

    # Jober.start will be called upon app start
    jober.start.assert_called_once()
    # Jober.stop will be called upon app shutdown
    jober.stop.assert_called_once()

    Jober._instance = None


def test_setup():
    app.state.setup(spec = 'foo')
    assert Jober.spec == 'foo'
    Jober._instance = None
    Jober.spec = None


def test_get_jobs():
    with Client() as client:
        # initialy no jobs
        data = client.get('/api/job/jobs').json()
        assert data['jobs'] == []

        # make a new jobs
        client.post('/api/job/make', json = {
            'name': 'foo',
        })

        # return the made jobs
        jobs = client.get('/api/job/jobs').json()['jobs']
        assert len(jobs) == 1
        job = jobs[0]
        assert job['name'] == 'foo'


def test_get_job():
    with Client() as client:
        # not existed
        res = client.get('/api/job/info', params = {'id': 'foo'})
        assert res.status_code == 404

        client.post('/api/job/make', json = {'id': 'foo'})
        res = client.get('/api/job/info', params = {'id': 'foo'})
        assert res.status_code == 200
        assert res.json()['id'] == 'foo'


def test_add_job():
    with Client() as client:
        # make some jobs
        res = client.post('/api/job/make', json = {'name': 'foo'})
        assert res.status_code == 200


def test_run_job():
    with Client() as client:
        res = client.post('/api/job/run', json = {'name': 'foo'})
        assert res.status_code == 404


class Client:

    def __enter__(self):
        self.client = TestClient(app)
        self.client.__enter__()
        return self.client

    def __exit__(self, *args, **kwargs):
        self.client.__exit__(*args, **kwargs)
        Jober._instance = None
