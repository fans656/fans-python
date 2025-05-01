import sys
import json
import contextlib
import subprocess
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

        # <-- setup server paths so client can use name to specify database instead of file path
        sync.setup_server(paths=paths)

        # <-- call `sync` to retrieve data from server
        sync({
            'origin': 'http://example.com',  # by default request /api/fans-sync
            'type': 'sqlite',
            'database': 'crawl_remote',
            'table': 'worth',
            'local_database_path': str(paths.crawl_local),
        })
        
        assert paths.crawl_local.exists()
    
    def test_cli(self, tmp_path):
        paths = make_paths(tmp_path, [
            'remote', {'name': 'remote', 'create': 'dir'}, [
                'remote.sqlite', {'crawl_remote'},
            ],
            'local', {'name': 'local', 'create': 'dir'}, [
                'local.sqlite', {'crawl_local'},
            ],
        ])
        self.prepare_remote_database(paths.crawl_remote)
        
        port = 12345

        cmd = f'{sys.executable} -m fans.sync --serve --port {port} --root {paths.remote}'
        server_proc = subprocess.Popen(cmd, shell=True)
        try:
            server_proc.wait(0.3)
        except subprocess.TimeoutExpired:
            pass

        req = json.dumps({
            'origin': f'http://localhost:{port}',
            'type': 'sqlite',
            'database': f'{paths.crawl_remote}',
            'table': 'worth',
            'local_database_path': f'{paths.crawl_local}',
        })
        cmd = f"{sys.executable} -m fans.sync '{req}'"
        client_proc = subprocess.Popen(cmd, shell=True)
        client_proc.wait()

        server_proc.terminate()
        server_proc.wait()

        assert paths.crawl_local.exists()

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
