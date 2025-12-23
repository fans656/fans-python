import sys
import time
import json
import threading
import contextlib

import yaml
import pytest
from starlette.testclient import TestClient
from fans.bunch import bunch
from fans.fn import noop
from async_asgi_testclient import TestClient as AsyncTestClient
import pytest
import pytest_asyncio

from fans.jober.app import app
from fans.jober.jober import Jober


@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client


@pytest_asyncio.fixture
async def async_client() -> AsyncTestClient:
    async with AsyncTestClient(app) as client:
        yield client


@pytest.fixture
def jober():
    with use_jober() as jober:
        yield jober


@contextlib.contextmanager
def use_jober(**conf):
    jober = Jober(**conf)
    Jober._instance = jober
    jober.start()
    yield jober
    jober.stop()
    Jober._instance = None


class Test_list_jobs:

    def test_empty_jobs_by_default(self, client):
        assert client.get('/api/jobs').json()['data'] == []
    
    def test_list_jobs(self, jober, client):
        jober.add_job(noop)
        jober.add_job(noop)

        jobs = client.get('/api/jobs').json()['data']

        assert len(jobs) == 2
        for job in jobs:
            assert 'id' in job
            assert 'status' in job


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

        runs = client.get('/api/runs', params={
            'job_id': job.id,
        }).json()['data']

        assert len(runs) == 2
        for run in runs:
            assert 'job_id' in run
            assert 'run_id' in run
            assert 'status' in run
            assert 'beg_time' in run
            assert 'end_time' in run


class Test_info:

    def test_info(self, client, tmp_path):
        conf_path = tmp_path / 'conf.yaml'
        with conf_path.open('w') as f:
            yaml.dump({}, f)

        with use_jober(conf_path=conf_path):
            data = client.get('/api/info').json()
            
            # can get conf path
            assert data['conf_path'] == str(conf_path)


class Test_prune_jobs:
    
    def test_prune(self, jober, mocker, client):
        job = jober.run_job(noop)
        job.wait()
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


class Test_logs:
    
    @pytest.mark.parametrize('capture', ['default', 'file'])
    async def test_head_tail(self, capture, client, tmp_path):
        with use_jober(root=tmp_path) as jober:

            def func():
                for i in range(3):
                    print(i)

            job = jober.run_job(func, capture=capture)
            job.wait()

            assert client.get("/api/logs", params={
                'job_id': job.id,
                'head': 2,
            }).text == '0\n1\n'

            assert client.get("/api/logs", params={
                'job_id': job.id,
                'head': 999,
            }).text == '0\n1\n2\n'

            assert client.get("/api/logs", params={
                'job_id': job.id,
                'tail': 2,
            }).text == '1\n2\n'

            assert client.get("/api/logs", params={
                'job_id': job.id,
                'tail': 999,
            }).text == '0\n1\n2\n'


class Test_logs_follow:
    
    @pytest.mark.parametrize('capture', ['default', 'file'])
    async def test_follow(self, capture, async_client, tmp_path):
        with use_jober(root=tmp_path) as jober:
            controller = threading.Event()
            
            def func():
                for i in range(3):
                    print(f'foo-{i}')
                    controller.wait()
                    controller.clear()

            job = jober.run_job(func, capture=capture)
            
            resp = await async_client.get("/api/logs", query_string={
                'job_id': job.id,
                'follow': True,
            }, stream=True)

            events = async_events(resp)
            
            assert (await anext(events))['line'] == 'foo-0\n'
            controller.set()

            assert (await anext(events))['line'] == 'foo-1\n'
            controller.set()

            assert (await anext(events))['line'] == 'foo-2\n'
            controller.set()
    
    async def test_follow_stderr(self, async_client, tmp_path):
        with use_jober(root=tmp_path) as jober:
            def func():
                print('foo')
                print('bar', file=sys.stderr)

            job = jober.run_job(func, capture=[':memory:', ':memory:'])
            
            resp = await async_client.get("/api/logs", query_string={
                'job_id': job.id,
                'follow': True,
            }, stream=True)
            events = async_events(resp)
            assert (await anext(events))['line'] == 'foo\n'
            
            run = job.last_run
            resp = await async_client.get("/api/logs", query_string={
                'job_id': job.id,
                'follow': True,
                'stderr': True,
            }, stream=True)
            events = async_events(resp)
            assert (await anext(events))['line'] == 'bar\n'
    
    @pytest.mark.parametrize('capture', ['default', 'file'])
    async def test_tail(self, capture, async_client, tmp_path):
        with use_jober(root=tmp_path) as jober:
            controller = threading.Event()
            
            def func():
                for i in range(6):
                    print(i)
                    controller.wait()
                    controller.clear()

            job = jober.run_job(func, capture=capture)
            
            controller.set()  # out 1
            time.sleep(0.01)
            controller.set()  # out 2
            time.sleep(0.01)
            controller.set()  # out 3
            time.sleep(0.01)
            
            resp = await async_client.get("/api/logs", query_string={
                'job_id': job.id,
                'follow': True,
                'tail': 2,
            }, stream=True)

            events = async_events(resp)
            
            assert (await anext(events))['line'] == '2\n'
            assert (await anext(events))['line'] == '3\n'

            controller.set()  # out 4
            assert (await anext(events))['line'] == '4\n'

            controller.set()  # out 5
            assert (await anext(events))['line'] == '5\n'

            controller.set()


class Test_ui_related:
    
    def test_running_status(self, jober, client):
        func = self.make_func()

        # not running yet
        job = jober.add_job(func)
        assert client.get('/api/jobs').json()['data'][0]['status'] == 'init'

        # running
        jober.run_job(job)
        job.wait(until='running')
        assert client.get('/api/jobs').json()['data'][0]['status'] == 'running'
        
        # done
        func.go()
        job.wait()
        assert client.get('/api/jobs').json()['data'][0]['status'] == 'done'
    
    async def test_status_events(self, jober, async_client):
        funcs = self.make_funcs(2)
        
        try:
            job0 = jober.add_job(funcs[0])
            job1 = jober.run_job(funcs[1]).wait(until='running')
            
            resp = await async_client.get('/api/events', stream=True)
            events = async_events(resp)
            init_event = await anext(events)
            
            # init event containing all job's status
            jobs = init_event['jobs']
            assert jobs[job0.id]['status'] == 'init'
            assert jobs[job1.id]['status'] == 'running'

            funcs[1].go()
            job1.wait(until='done')

            # can receive status change event
            event = await anext(events)
            assert event['type'] == 'run_status'
            assert event['status'] == 'done'
            assert event['job_id'] == job1.id
        finally:
            funcs[1].go()
    
    def make_funcs(self, n):
        return [self.make_func() for _ in range(n)]
    
    def make_func(self):
        event = threading.Event()

        def func():
            event.wait()
        
        func.go = lambda: event.set()
        
        return func
    

async def async_events(resp):
    async for chunk in resp.iter_content(chunk_size=None):
        line = chunk.decode()
        if line.startswith('data:'):
            yield json.loads(line[5:].strip())
