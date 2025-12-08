import pytest
from starlette.testclient import TestClient

from fans.dbutil.nos.service import Service
from fans.dbutil.nos.app import app


def test_info(client):
    assert client.get('/api/nos/info').json() == {
        'stores': [{
            'name': 'default',
            'path': ':memory:',
        }],
    }


def test_crud(client):
    assert client.get('/api/nos/count').json() == 0

    assert client.post('/api/nos/put', json={
        'name': 'foo',
        'age': 3,
    }).status_code == 200

    assert client.get('/api/nos/count').json() == 1

    assert client.get('/api/nos/get', params={
        'key': 'foo',
    }).json() == {'name': 'foo', 'age': 3}

    assert client.post('/api/nos/update', params={'key': 'foo'}, json={
        'age': 5,
    }).status_code == 200

    assert client.get('/api/nos/get', params={
        'key': 'foo',
    }).json() == {'name': 'foo', 'age': 5}

    assert client.post('/api/nos/put', json={
        'name': 'bar',
        'age': 7,
    }).status_code == 200

    assert client.get('/api/nos/list').json() == [
        {'name': 'foo', 'age': 5},
        {'name': 'bar', 'age': 7},
    ]

    assert client.post('/api/nos/remove', params={'key': 'foo'}).status_code == 200
    
    assert client.get('/api/nos/count').json() == 1


@pytest.fixture
def client():
    Service.get_instance(fresh=True)  # ensure fresh service
    with TestClient(app) as client:
        yield client
