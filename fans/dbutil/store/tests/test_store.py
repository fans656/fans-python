import peewee

from fans.dbutil.store import Store


def test_init(tmp_path):
    assert Store(':memory:').database
    assert Store(tmp_path / 'data.sqlite').database

    database = peewee.SqliteDatabase(':memory:')
    assert Store(database).database is database


def test_get_collection():
    store = Store(':memory:')
    collection = store.get_collection('foo')
    assert store.get_collection('foo') is collection
