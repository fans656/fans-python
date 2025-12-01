import peewee

from fans.dbutil.store import Store


def test_constructor(tmp_path):
    # make store by `str`
    assert Store(':memory:').database
    
    # make store by `Path`
    assert Store(tmp_path / 'data.sqlite').database

    # make store by existing database
    database = peewee.SqliteDatabase(':memory:')
    assert Store(database).database is database


def test_get_collection():
    store = Store(':memory:')
    
    # get a fresh collection
    collection = store.get_collection('foo')
    
    # later get is cached
    assert store.get_collection('foo') is collection
