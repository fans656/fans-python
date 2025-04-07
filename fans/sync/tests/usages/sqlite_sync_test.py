import contextlib
from urllib.parse import urlparse

import peewee
import pytest
from fans.path import make_paths
from starlette.testclient import TestClient

from fans.sync import sync
from fans.sync.app import app
from fans.sync.sqlite_sync import handle_sqlite_sync_client_side


@pytest.fixture
def client(mocker):
    client = TestClient(app)
    
    def post(url, *args, **kwargs):
        path = urlparse(url).path
        return client.post(path, *args, **kwargs)

    mocker.patch('requests.post', new=post)

    yield client
    sync.reset()


@contextlib.contextmanager
def setup_server(*args, **kwargs):
    sync.server.setup(*args, **kwargs)
    yield
    sync.reset()


class TestDefault:

    def test_default(self, client, tmp_path):
        paths = make_paths(tmp_path, [
            'remote', {'create': 'dir'}, [
                'remote.sqlite', {'crawl_remote'},
            ],
            'local', {'create': 'dir'}, [
                'local.sqlite', {'crawl_local'},
            ],
        ])
        self.prepare_remote_database(paths.crawl_remote)

        sync.setup_server(paths=paths)

        sync({
            'origin': 'http://example.com',
            'type': 'sqlite',
            'database': 'crawl_remote',
            'table': 'worth',
            'local_database_path': str(paths.crawl_local),
        })

    def prepare_remote_database(self, database_path):
        
        class Worth(peewee.Model):

            date = peewee.TextField(primary_key=True)
            unit_worth = peewee.FloatField()
            added = peewee.IntegerField(index=True)

        database = peewee.SqliteDatabase(database_path)
        tables = [Worth]
        database.bind(tables)
        database.create_tables(tables)
        
        Worth.insert_many([
            {'date': '2025-01-01', 'unit_worth': 3.1, 'added': 1},
            {'date': '2025-01-02', 'unit_worth': 3.2, 'added': 3},
        ]).execute()
