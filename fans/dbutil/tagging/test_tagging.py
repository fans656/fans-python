import pytest
import peewee

from fans import dbutil
from fans.dbutil.tagging.tagging import (
    _ensure_flat_tuples,
)


_confs = [
    {'composite': False},
    {'composite': True},
]


@pytest.fixture
def tagging(request):
    conf = request.node.callspec.params['conf']
    if conf['composite']:
        return dbutil.tagging(peewee.SqliteDatabase(':memory:'), key=(int, float))
    else:
        return dbutil.tagging(peewee.SqliteDatabase(':memory:'))


@pytest.fixture
def k(request):
    conf = request.node.callspec.params['conf']
    if conf['composite']:
        return lambda d: (d, float(d))
    else:
        return lambda d: d


def test_usage():
    """
    -----------------------------------------------------------------
    0   even                    square  cube
    -----------------------------------------------------------------
    1           odd             square  cube                factorial
    -----------------------------------------------------------------
    2   even            prime                               factorial
    -----------------------------------------------------------------
    3           odd     prime
    -----------------------------------------------------------------
    4   even                    square
    -----------------------------------------------------------------
    5           odd     prime
    -----------------------------------------------------------------
    6   even                                    perfect     factorial
    -----------------------------------------------------------------
    7           odd     prime
    -----------------------------------------------------------------
    8   even                            cube
    -----------------------------------------------------------------
    9           odd             square
    -----------------------------------------------------------------
    """
    db = peewee.SqliteDatabase(':memory:')

    tagging = dbutil.tagging(db)

    tagging.tag([0, 2, 4, 6, 8], 'even')
    tagging.tag([1, 3, 5, 7, 9], 'odd')
    tagging.tag([2, 3, 5, 7], 'prime')
    tagging.tag([0, 1, 4, 9], 'square')
    tagging.tag([0, 1, 8], 'cube')
    tagging.tag(6, 'perfect')
    tagging.tag([1, 2, 6], 'factorial')

    # single tag expr
    assert set(tagging.find('prime')) == {2,3,5,7}

    # simple OR expr
    assert set(tagging.find('cube | square')) == {0,1,4,8,9}

    # simple AND expr
    assert set(tagging.find('prime factorial')) == {2}

    # complex
    assert set(tagging.find('(cube | square) even')) == {0,4,8}
    assert set(tagging.find('odd (cube | square)')) == {1,9}
    assert set(tagging.find('even !factorial !cube')) == {4}

    # test get tags
    assert set(tagging.tags(0)) == {'even', 'square', 'cube'}
    assert set(tagging.tags(1)) == {'odd', 'square', 'cube', 'factorial'}
    assert set(tagging.tags(6)) == {'even', 'perfect', 'factorial'}
    assert set(tagging.tags()) == {'even', 'odd', 'prime', 'square', 'cube', 'perfect', 'factorial'}


def test_specify_table_name():
    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db)
    assert 'tag' in set(db.get_tables())

    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db, 'foo')
    assert 'foo' in set(db.get_tables())
    assert 'tag' not in set(db.get_tables())


def test_can_return_query():
    db = peewee.SqliteDatabase(':memory:')

    class Entity(peewee.Model):

        key = peewee.IntegerField(primary_key=True)
        name = peewee.TextField()

    db.bind([Entity])
    db.create_tables([Entity])

    Entity.insert_many([
        {'key': 1, 'name': 'Alice'},
        {'key': 2, 'name': 'Bob'},
    ]).execute()

    tagging = dbutil.tagging(db)
    tagging.tag(1, 'foo')
    tagging.tag(1, 'bar')
    tagging.tag(2, 'bar')
    tagging.tag(2, 'baz')

    sub_query = tagging.find('foo', return_query=True)
    query = Entity.select(Entity.name).where(Entity.key << sub_query)
    assert set([d.name for d in query]) == {'Alice'}

    sub_query = tagging.find('bar', return_query=True)
    query = Entity.select(Entity.name).where(Entity.key << sub_query)
    assert set([d.name for d in query]) == {'Alice', 'Bob'}


def test_key_types():
    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db)
    assert list(db.execute_sql('pragma table_info(tag)'))[0][2] == 'INTEGER'

    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db, key=str)
    assert list(db.execute_sql('pragma table_info(tag)'))[0][2] == 'TEXT'

    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db, key=float)
    assert list(db.execute_sql('pragma table_info(tag)'))[0][2] == 'REAL'


def test_composite_key():
    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db, key=(float, str))

    tagging.tag((1.5, 'foo'), 'red')
    tagging.tag((1.5, 'bar'), 'red')
    tagging.tag((3.0, 'baz'), 'blue')

    assert set(tagging.find('red')) == {(1.5, 'foo'), (1.5, 'bar')}
    assert set(tagging.find('blue')) == {(3.0, 'baz')}

    assert set(tagging.tags()) == {'red', 'blue'}


class Test_tag:
    
    @pytest.mark.parametrize('conf', _confs)
    def test_single_key_single_tag(self, tagging, k, conf):
        tagging.tag(k(1), 'foo')
        assert set(tagging.find('foo')) == {k(1)}
    
    @pytest.mark.parametrize('conf', _confs)
    def test_single_key_multiple_tags(self, tagging, k, conf):
        tagging.tag(k(1), 'foo', 'bar')
        assert set(tagging.find('foo')) == {k(1)}
        assert set(tagging.find('bar')) == {k(1)}
    
    @pytest.mark.parametrize('conf', _confs)
    def test_multiple_keys_single_tag(self, tagging, k, conf):
        tagging.tag([k(1), k(2)], 'foo')
        assert set(tagging.find('foo')) == {k(1), k(2)}
    
    @pytest.mark.parametrize('conf', _confs)
    def test_multiple_keys_multiple_tags(self, tagging, k, conf):
        tagging.tag([k(1), k(2)], 'foo', 'bar')
        assert set(tagging.find('foo')) == {k(1), k(2)}
        assert set(tagging.find('bar')) == {k(1), k(2)}
    
    @pytest.mark.parametrize('conf', _confs)
    def test_batch_add(self, tagging, k, conf):
        tagging.tag([
            (k(1), 'foo'),
            (k(1), 'bar'),
            (k(2), 'foo'),
        ])
        assert set(tagging.find('foo')) == {k(1), k(2)}
        assert set(tagging.find('bar')) == {k(1)}

    def test_batch_add_flatten_composite_key(self):
        tagging = dbutil.tagging(peewee.SqliteDatabase(':memory:'), key=(int, float))
        tagging.tag([
            (1, 1.0, 'foo'),
            (1, 1.0, 'bar'),
            (2, 2.0, 'foo'),
        ])
        assert set(tagging.find('foo')) == {(1, 1.0), (2, 2.0)}
        assert set(tagging.find('bar')) == {(1, 1.0)}


class Test_untag:
    
    @pytest.mark.parametrize('conf', _confs)
    def test_single_key_single_tag(self, tagging, k, conf):
        tagging.tag(k(1), 'foo')
        tagging.tag(k(1), 'bar')
        tagging.untag(k(1), 'foo')
        assert not tagging.find('foo')
        assert set(tagging.find('bar')) == {k(1)}
    
    @pytest.mark.parametrize('conf', _confs)
    def test_single_key_multiple_tags(self, tagging, k, conf):
        tagging.tag(k(1), 'foo', 'bar', 'baz')
        tagging.untag(k(1), 'foo', 'bar')
        assert not tagging.find('foo')
        assert not tagging.find('bar')
        assert set(tagging.find('baz')) == {k(1)}
    
    @pytest.mark.parametrize('conf', _confs)
    def test_single_key_all_tags(self, tagging, k, conf):
        tagging.tag(k(1), 'foo', 'bar')
        tagging.tag(k(2), 'bar')
        tagging.untag(k(1))
        assert not tagging.find('foo')
        assert set(tagging.find('bar')) == {k(2)}
    
    @pytest.mark.parametrize('conf', _confs)
    def test_multiple_keys_single_tag(self, tagging, k, conf):
        tagging.tag(k(1), 'foo')
        tagging.tag(k(2), 'foo')
        tagging.tag(k(3), 'foo')
        tagging.tag(k(1), 'bar')
        tagging.untag([k(1), k(2)], 'foo')
        assert set(tagging.find('foo')) == {k(3)}
        assert set(tagging.find('bar')) == {k(1)}
    
    @pytest.mark.parametrize('conf', _confs)
    def test_multiple_keys_mutilpe_tags(self, tagging, k, conf):
        tagging.tag(k(1), 'foo')
        tagging.tag(k(2), 'foo')
        tagging.tag(k(3), 'foo')
        tagging.tag(k(1), 'bar')
        tagging.tag(k(1), 'baz')
        tagging.untag([k(1), k(2)], 'foo', 'bar')
        assert set(tagging.find('foo')) == {k(3)}
        assert not tagging.find('bar')
        assert set(tagging.find('baz')) == {k(1)}
    
    @pytest.mark.parametrize('conf', _confs)
    def test_multiple_keys_all_tags(self, tagging, k, conf):
        tagging.tag(k(1), 'foo')
        tagging.tag(k(2), 'foo')
        tagging.tag(k(3), 'foo')
        tagging.tag(k(1), 'bar')
        tagging.tag(k(1), 'baz')
        tagging.untag([k(1), k(2)])
        assert set(tagging.find('foo')) == {k(3)}
        assert not tagging.find('bar')
        assert not tagging.find('baz')
    
    @pytest.mark.parametrize('conf', _confs)
    def test_chunked(self, tagging, k, conf):
        n = 100
        tagging.tag([k(i) for i in range(n)], 'foo')
        tagging.untag([k(i) for i in range(n - 1)])
        assert set(tagging.find('foo')) == {k(n - 1)}


class Test_derive_tagging_table_from_target:

    def test_simple_key(self):
        database = peewee.SqliteDatabase(':memory:')
        
        class Person(peewee.Model):
            
            name = peewee.TextField(primary_key=True)
            age = peewee.IntegerField()
        
        database.bind([Person])
        database.create_tables([Person])
        
        tagging = dbutil.tagging(database, target='person')
        tagging.tag([
            ('foo', 'f'),
            ('bar', 'b'),
        ])
        assert set(tagging.find('f')) == {'foo'}
        assert set(tagging.find('b')) == {'bar'}

        assert tagging.table_name == 'person_tag'
        assert set(tagging.model._meta.columns.keys()) == {'name', 'tag'}

    def test_composite_key(self):
        database = peewee.SqliteDatabase(':memory:')
        
        class Person(peewee.Model):
            
            class Meta:
                
                primary_key = peewee.CompositeKey('first_name', 'last_name')
            
            first_name = peewee.TextField()
            last_name = peewee.TextField()
            age = peewee.IntegerField()
        
        database.bind([Person])
        database.create_tables([Person])
        
        tagging = dbutil.tagging(database, target='person')
        tagging.tag([
            ('foo', 'oo', 'f'),
            ('bar', 'rr', 'b'),
        ])
        assert set(tagging.find('f')) == {('foo', 'oo')}
        assert set(tagging.find('b')) == {('bar', 'rr')}

        assert tagging.table_name == 'person_tag'
        assert set(tagging.model._meta.columns.keys()) == {'first_name', 'last_name', 'tag'}

    def test_occupied_tag_column_name(self):
        database = peewee.SqliteDatabase(':memory:')
        
        class Foo(peewee.Model):
            
            class Meta:
                
                primary_key = peewee.CompositeKey('uid', 'tag')
            
            uid = peewee.TextField()
            tag = peewee.IntegerField()
        
        database.bind([Foo])
        database.create_tables([Foo])
        
        tagging = dbutil.tagging(database, target='foo')
        assert set(tagging.model._meta.columns.keys()) == {'uid', 'tag', 'tag0'}


def test_ensure_flat_tuples():
    assert list(_ensure_flat_tuples([])) == []

    assert list(_ensure_flat_tuples((1, 2, 3))) == [1, 2, 3]
    assert list(_ensure_flat_tuples([1, 2, 3])) == [1, 2, 3]

    assert list(_ensure_flat_tuples([
        (1, 'a'),
        (2, 'b'),
    ])) == [
        (1, 'a'),
        (2, 'b'),
    ]

    assert list(_ensure_flat_tuples([
        ((1, 1), 'a'),
        ((2, 2), 'b'),
    ])) == [
        (1, 1, 'a'),
        (2, 2, 'b'),
    ]
