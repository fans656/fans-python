import pytest
from starlette.testclient import TestClient

from fans.dbutil.nos.service import Service
from fans.dbutil.nos.app import app


def test_info(client):
    info = client.get('/api/nos/info').json()
    assert 'stores' in info
    store = info['stores'][0]
    assert store['name'] == 'default'
    assert store['path'] == ':memory:'


def test_put(client):
    assert client.post('/api/nos/put', json={
        'name': 'foo',
        'age': 3,
    }).status_code == 200
    assert client.get('/api/nos/count').json() == 1


def test_count(client):
    assert client.get('/api/nos/count').json() == 0


@pytest.fixture
def client():
    Service.get_instance(fresh=True)  # ensure fresh service
    with TestClient(app) as client:
        yield client
