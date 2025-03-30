import pytest
from starlette.testclient import TestClient

from .app import app


@pytest.fixture
def client():
    yield TestClient(app)


class Test_default:

    def test_default(self, client):
        res = client.post('/api/fans-sync', json={
            'syncs': [
                'fans.sync.tests.samples.sync',
            ],
        })
