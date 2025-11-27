import pytest
import peewee

from fans.dbutil.store.collection import Collection


def test_simple_usage():
    c = Collection('foo', peewee.SqliteDatabase(':memory:'))

    c.put({'name': 'foo', 'age': 3})
    c.put({'name': 'bar', 'age': 5})
    
    assert c.get('foo') == {'name': 'foo', 'age': 3}
    assert c.get('bar') == {'name': 'bar', 'age': 5}

    assert len(c) == 2
    c.remove('foo')
    assert len(c) == 1


class Test_get:
    
    def test_simple_key(self, c):
        c.put({'id': 1, 'val': 1})
        assert c.get(1) == {'id': 1, 'val': 1}  # get single

        c.put({'id': 2, 'val': 2})
        assert c.get([1, 2]) == [{'id': 1, 'val': 1}, {'id': 2, 'val': 2}]  # get multiple


class Test_put:
    
    def test_single_item(self, c):
        c.put({'id': 1, 'val': 1})
        assert c.get(1) == {'id': 1, 'val': 1}
    
    def test_multiple_items(self, c):
        c.put([
            {'id': 1, 'val': 1},
            {'id': 2, 'val': 2},
        ])
        assert c.get(1) == {'id': 1, 'val': 1}
        assert c.get(2) == {'id': 2, 'val': 2}
    
    def test_on_conflict(self, c):
        pass  # TODO


class Test_remove:
    
    def test_default(self, c):
        pass  # TODO


class Test_option_key:
    
    def test_default(self, c):
        c.put({'id': '1', 'val': 1})
        assert c.get('1') == {'id': '1', 'val': 1}

        c.put({'key': '2', 'val': 2})
        assert c.get('2') == {'key': '2', 'val': 2}

        c.put({'name': '3', 'val': 3})
        assert c.get('3') == {'name': '3', 'val': 3}

    def test_specify_key(self):
        c = Collection('foo', peewee.SqliteDatabase(':memory:'), key='uid')
        c.put({'uid': '1', 'val': 1})
        assert c.get('1') == {'uid': '1', 'val': 1}

    def test_specify_keys(self):
        c = Collection('foo', peewee.SqliteDatabase(':memory:'), key=['uid', 'uuid'])

        c.put({'uid': '1', 'val': 1})
        assert c.get('1') == {'uid': '1', 'val': 1}

        c.put({'uuid': '2', 'val': 2})
        assert c.get('2') == {'uuid': '2', 'val': 2}


class Test_misc:

    def test_option_override(self):
        c = Collection('foo', peewee.SqliteDatabase(':memory:'), on_conflict='ignore')
        assert c._opt('on_conflict') == 'ignore'
        assert c._opt('on_conflict', {'on_conflict': 'replace'}) == 'replace'


@pytest.fixture
def c():
    return Collection('foo', peewee.SqliteDatabase(':memory:'), auto_key_type=peewee.IntegerField)
