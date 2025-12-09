import json

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
        {'name': 'bar', 'age': 7},
        {'name': 'foo', 'age': 5},
    ]

    assert client.post('/api/nos/remove', params={'key': 'foo'}).status_code == 200
    
    assert client.get('/api/nos/count').json() == 1


def test_tagging(client):
    item = lambda i: {'id': i, 'val': i}

    client.post('/api/nos/put', json=[item(i) for i in range(10)])
    assert client.get('/api/nos/count').json() == 10

    client.post('/api/nos/tag', json={
        'key': 6,
        'tag': 'perfect',
    })
    assert client.get('/api/nos/find', params={'query': 'perfect'}).json() == [item(6)]

    client.post('/api/nos/tag', json={
        'key': [0,1,8],
        'tag': 'cube',
    })
    assert client.get('/api/nos/find', params={'query': 'cube'}).json() == [item(d) for d in [0,1,8]]

    client.post('/api/nos/tag', json={
        'key': [0,2,4,6,8],
        'tag': 'even',
    })
    assert client.get('/api/nos/find', params={
        'query': 'even cube',
    }).json() == [item(d) for d in [0,8]]

    client.post('/api/nos/untag', json={
        'key': 6,
        'tag': 'perfect',
    })
    assert client.get('/api/nos/find', params={'query': 'perfect'}).json() == []

    assert set(client.get('/api/nos/tags').json()) == {'even', 'cube'}


class Test_put:
    
    def test_options(self, client):
        client.post('/api/nos/put', json={'name': 'foo', 'age': 3})

        client.post('/api/nos/put', json={'name': 'foo', 'age': 5})
        assert client.get('/api/nos/get', params={'key': 'foo'}).json() == {'name': 'foo', 'age': 5}

        client.post('/api/nos/put', json={'name': 'foo', 'age': 7}, params={
            'options': json.dumps({'on_conflict': 'ignore'}),
        })
        assert client.get('/api/nos/get', params={'key': 'foo'}).json() == {'name': 'foo', 'age': 5}


class Test_get:
    
    def test_multiple(self, client):
        client.post('/api/nos/put', json=[
            {'name': 'foo', 'age': 3},
            {'name': 'bar', 'age': 5},
            {'name': 'baz', 'age': 7},
        ])
        assert client.get('/api/nos/get', params={
            'key': '["foo", "bar"]',
        }).json() == [
            {'name': 'foo', 'age': 3},
            {'name': 'bar', 'age': 5},
        ]
    
    def test_composite_key(self, client):
        client.post('/api/nos/create_store', json={
            'name': 'foo',
            'collections': {
                'default': {
                    'fields': {
                        'node_id': 'int',
                        'time_pos': 'float',
                        'tag': 'str',
                    },
                    'primary_key': ['node_id', 'time_pos'],
                },
            },
        })
        client.post('/api/nos/put', json={
            'node_id': 1,
            'time_pos': 5.0,
            'tag': 'foo',
        }, params={'store': 'foo'})

        assert client.get('/api/nos/get', params={
            'key': '[1, 5.0]',
            'store': 'foo',
        }).json() == {'node_id': 1, 'time_pos': 5.0, 'tag': 'foo'}

        assert client.get('/api/nos/get', params={
            'key': '[[1, 5.0]]',
            'store': 'foo',
        }).json() == [{'node_id': 1, 'time_pos': 5.0, 'tag': 'foo'}]


class Test_update:
    
    def test_composite_key(self, client):
        client.post('/api/nos/create_store', json={
            'name': 'foo',
            'collections': {
                'default': {
                    'fields': {
                        'node_id': 'int',
                        'time_pos': 'float',
                        'tag': 'str',
                    },
                    'primary_key': ['node_id', 'time_pos'],
                },
            },
        })
        client.post('/api/nos/put', json={
            'node_id': 1,
            'time_pos': 5.0,
            'tag': 'foo',
        }, params={'store': 'foo'})

        client.post('/api/nos/update', json={
            'tag': 'bar',
        }, params={
            'key': '[1, 5.0]',
            'store': 'foo',
        })

        assert client.get('/api/nos/get', params={
            'key': '[[1, 5.0]]',
            'store': 'foo',
        }).json() == [{'node_id': 1, 'time_pos': 5.0, 'tag': 'bar'}]


class Test_remove:
    
    def test_composite_key(self, client):
        client.post('/api/nos/create_store', json={
            'name': 'foo',
            'collections': {
                'default': {
                    'fields': {
                        'node_id': 'int',
                        'time_pos': 'float',
                        'tag': 'str',
                    },
                    'primary_key': ['node_id', 'time_pos'],
                },
            },
        })
        client.post('/api/nos/put', json=[
            {'node_id': 1, 'time_pos': 1.0, 'tag': '1'},
            {'node_id': 2, 'time_pos': 2.0, 'tag': '2'},
            {'node_id': 3, 'time_pos': 3.0, 'tag': '3'},
        ], params={'store': 'foo'})

        client.post('/api/nos/remove', params={
            'key': '[1, 1.0]',
            'store': 'foo',
        })
        assert client.get('/api/nos/get', params={
            'key': '[1, 1.0]',
            'store': 'foo',
        }).json() == None

        client.post('/api/nos/remove', params={
            'key': '[[2, 2.0], [3, 3.0]]',
            'store': 'foo',
        })
        assert client.get('/api/nos/get', params={
            'key': '[[2, 2.0], [3, 3.0]]',
            'store': 'foo',
        }).json() == []


def test_choose_store_and_collection(client):
    client.post('/api/nos/create_store', json={'name': 'foo'})
    client.post('/api/nos/put', json={'name': 'foo', 'age': 3}, params={
        'store': 'foo',
        'collection': 'person',
    })

    assert client.get('/api/nos/get', params={
        'key': 'foo',
    }).json() == None
    assert client.get('/api/nos/get', params={
        'key': 'bar',
    }).json() == None
    assert client.get('/api/nos/get', params={
        'key': 'foo',
        'store': 'foo',
    }).json() == None
    assert client.get('/api/nos/get', params={
        'key': 'foo',
        'store': 'foo',
        'collection': 'person',
    }).json() == {'name': 'foo', 'age': 3}


@pytest.fixture
def client():
    Service.get_instance(fresh=True)  # ensure fresh service
    with TestClient(app) as client:
        yield client
