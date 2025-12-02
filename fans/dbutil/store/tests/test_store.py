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


def test_store_level_options():
    store = Store(':memory:', on_conflict='ignore')

    # inherit store level options
    c1 = store.get_collection('c1')
    c1.put({'id': '1', 'val': 'one'})
    c1.put({'id': '1', 'val': 'two'})
    assert c1.get('1') == {'id': '1', 'val': 'one'}

    # can specify overrides when get_collection
    c2 = store.get_collection('c2', on_conflict='replace')
    c2.put({'id': '1', 'val': 'one'})
    c2.put({'id': '1', 'val': 'two'})
    assert c2.get('1') == {'id': '1', 'val': 'two'}
