import json

import pytest
import peewee

from fans.dbutil.store.collection import (
    Collection,
    _set_options_defaults,
    _normalized_fields,
)
from fans.dbutil import migrate


CONFS = [
    {'composite_key': False},
    {'composite_key': True},
]


class Test_readme:

    def test_usage_basic(self):
        c = Collection('foo', peewee.SqliteDatabase(':memory:'))

        # put items into collection
        c.put({'name': 'foo', 'age': 3})
        c.put({'name': 'bar', 'age': 5})
        
        # get item (by key)
        assert c.get('foo') == {'name': 'foo', 'age': 3}
        assert c.get('bar') == {'name': 'bar', 'age': 5}
        
        # update existing item
        c.update('foo', {'age': 7})
        assert c.get('foo') == {'name': 'foo', 'age': 7}

        # count items
        assert c.count() == 2

        # remove item (by key)
        c.remove('foo')
        assert c.count() == 1
    
    def test_specify_fields_as_separate_column(self):
        c = Collection('foo', **{
            'fields': {
                'age': {'type': 'int', 'index': True},
            },
        })

        fields = c.model._meta.fields
        assert len(fields) == 3
        assert c._auto_key_field in fields
        assert c._auto_data_field in fields
        assert 'age' in fields  # column added

        field = fields['age']
        assert field.index  # is index
    
    def test_specify_primary_key(self):
        c = Collection('foo', **{
            'fields': {
                'node_id': {'type': 'int'},
                'time_pos': {'type': 'float'},
            },
            'primary_key': ['node_id', 'time_pos'],
        })

        item1 = {'node_id': 123, 'time_pos': 60.0, 'tagging': 'foo bar'}
        item2 = {'node_id': 456, 'time_pos': 10.0, 'meta': '{}'}

        c.put(item1)
        c.put(item2)
        
        assert c.list() == [item1, item2]
        assert c.get((123, 60.0)) == item1
        assert c.get((456, 10.0)) == item2

    def test_use_existing_database_table(self):
        database = peewee.SqliteDatabase(':memory:')
        
        #-------------------- prepare database

        class Person(peewee.Model):
            
            class Meta:
                
                primary_key = peewee.CompositeKey('forename', 'surname')
            
            forename = peewee.TextField()
            surname = peewee.TextField()
        
        database.bind([Person])
        database.create_tables([Person])

        Person.insert_many([
            {'forename': 'Alex', 'surname': 'Honnold'},
            {'forename': 'Moby', 'surname': 'Dick'},
        ]).execute()

        #-------------------- collection from existing database

        c = Collection('person', database)

        # use tuple for composite key
        assert c.get(('Alex', 'Honnold')) == {'forename': 'Alex', 'surname': 'Honnold'}

        assert c.get([
            ('Moby', 'Dick'),
            ('Alex', 'Honnold'),
        ]) == [
            {'forename': 'Moby', 'surname': 'Dick'},
            {'forename': 'Alex', 'surname': 'Honnold'},
        ]


class Test_auto_migration:

    def test_add_column_remove_column(self):
        """
        1. Initially no separate column:
        
            _key    _data
            foo     {"name":"foo","age":3}
        
        2. Then specify 'age' as separate column:
        
            _key    _data           age
            foo     {"name":"foo"}  3

        3. Then remove 'age' column again (with 'gender' added, otherwise empty schema won't do migration):
        
            _key    _data                   gender
            foo     {"name":"foo","age":3}  null
        """
        database = peewee.SqliteDatabase(':memory:')

        # 1.
        c = Collection('foo', database)
        c.put({'name': 'foo', 'age': 3})

        # 2.
        c = Collection('foo', database, **{
            'fields': {
                'age': {'type': 'int', 'index': True},
            },
        })

        fields = c.model._meta.fields
        assert 'age' in fields  # column added
        field = fields['age']
        assert field.index  # is index
        
        row = c.model.get_by_id('foo')
        assert row.age == 3  # value populated
        assert 'age' not in json.loads(row._data)  # value removed from auto data field
        
        # put/get after table change
        c.put({'name': 'bar', 'age': 5})
        assert c.model.get_by_id('bar').age == 5
        assert c.get('bar') == {'name': 'bar', 'age': 5}

        # 3.
        c = Collection('foo', database, **{
            'fields': {
                'gender': {'type': 'str', 'index': True},
            },
        })
        fields = c.model._meta.fields
        assert 'age' not in fields  # column removed
        assert c.get('foo') == {'name': 'foo', 'age': 3, 'gender': None}  # value re-add into data field

    def test_change_field_type(self):
        database = peewee.SqliteDatabase(':memory:')

        c = Collection('foo', database, **{
            'fields': {
                'age': {'type': 'int'},
            },
        })
        c.put({'name': 'foo', 'age': 3})
        c.put({'name': 'bar', 'age': 13})
        assert [d.age for d in c.model.select().order_by(c.model.age)] == [3, 13]

        c = Collection('foo', database, **{
            'fields': {
                'age': {'type': 'str'},
            },
        })
        assert [d.age for d in c.model.select().order_by(c.model.age)] == ['13', '3']

    def test_add_index_remove_index(self):
        database = peewee.SqliteDatabase(':memory:')

        c = Collection('foo', database, **{
            'fields': {
                'name': {'type': 'str'},
                'gender': {'type': 'str'},
                'age': {'type': 'int'},
            },
        })
        assert 'foo_gender' not in {d.name for d in database.get_indexes('foo')}

        c = Collection('foo', database, **{
            'fields': {
                'name': {'type': 'str'},
                'gender': {'type': 'str', 'index': True},
                'age': {'type': 'int'},
            },
        })
        assert 'foo_gender' in {d.name for d in database.get_indexes('foo')}

        c = Collection('foo', database, **{
            'fields': {
                'name': {'type': 'str'},
                'gender': {'type': 'str'},
                'age': {'type': 'int'},
            },
        })
        assert 'foo_gender' not in {d.name for d in database.get_indexes('foo')}
    
    def test_change_primary_key(self):
        database = peewee.SqliteDatabase(':memory:')

        c = Collection('foo', database)
        assert c.model._meta.primary_key.column_name == '_key'

        c = Collection('foo', database, **{
            'fields': {
                'name': {'type': 'str', 'primary_key': True},
                'age': {'type': 'int'},
            },
        })
        assert c.model._meta.primary_key.column_name == 'name'

        c = Collection('foo', database, **{
            'fields': {
                'uid': {'type': 'str', 'primary_key': True},
                'name': {'type': 'str'},
                'age': {'type': 'int'},
            },
        })
        assert c.model._meta.primary_key.column_name == 'uid'

        c = Collection('foo', database, **{
            'fields': {
                'city': {'type': 'str'},
                'name': {'type': 'str'},
                'age': {'type': 'int'},
            },
            'primary_key': ['city', 'name'],
        })
        assert c.model._meta.primary_key.field_names == ('city', 'name')
    
    def test_rename_table(self):
        database = peewee.SqliteDatabase(':memory:')

        c = Collection('foo', database)
        assert database.get_tables() == ['foo']

        c = Collection('bar', database, old_name='foo')
        assert database.get_tables() == ['bar']
    
    def test_rename_column(self):
        database = peewee.SqliteDatabase(':memory:')

        c = Collection('foo', database, **{
            'fields': {
                'nodeid': 'int',
            },
        })
        assert 'nodeid' in c.model._meta.fields

        c = Collection('foo', database, **{
            'fields': {
                'node_id': {'type': 'int', 'old_name': 'nodeid'},
            },
        })
        assert 'nodeid' not in c.model._meta.fields
        assert 'node_id' in c.model._meta.fields


class Test_get:
    
    @pytest.mark.parametrize('conf', CONFS)
    def test_default(self, c, key, keys, item, conf):
        c.put(item(1))
        assert c.get(key(1)) == item(1)  # get single item by key

        c.put(item(2))
        assert c.get(keys([1, 2])) == [item(1), item(2)]  # get multiple items by keys
        assert c.get(keys([2, 1])) == [item(2), item(1)]  # same order as given keys
    
    def test_callable_query(self):
        c = Collection('person', fields={'age': {'type': 'int', 'index': True}})
        c.put({'name': 'foo', 'age': 3})
        c.put({'name': 'bar', 'age': 5})
        
        assert c.get(lambda m: m.select().where(m.age > 4))[0] == {'name': 'bar', 'age': 5}
    
    def test_callable_pred(self):
        c = Collection('person', fields={'age': {'type': 'int', 'index': True}})
        c.put({'name': 'foo', 'age': 3})
        c.put({'name': 'bar', 'age': 5})
        
        assert c.get(lambda m: m.age > 4)[0] == {'name': 'bar', 'age': 5}
    
    def test_raw(self):
        c = Collection('person', fields={'age': 'int'})
        c.put({'name': 'foo', 'age': 3})
        c.put({'name': 'bar', 'age': 5})

        assert isinstance(c.get('foo', raw=True), c.model)
        
        query = c.get(['foo', 'bar'], raw=True)
        assert isinstance(query, peewee.ModelSelect)
        assert all(isinstance(row, c.model) for row in query)

        query = c.get(lambda m: m.select().where(m.age > 4), raw=True)
        assert isinstance(query, peewee.ModelSelect)
        assert all(isinstance(row, c.model) for row in query)


class Test_put:
    
    @pytest.mark.parametrize('conf', CONFS)
    def test_single_item(self, c, key, item, conf):
        c.put(item(1))
        assert c.get(key(1)) == item(1)
    
    @pytest.mark.parametrize('conf', CONFS)
    def test_multiple_items(self, c, key, item, conf):
        c.put([item(1), item(2)])
        assert c.get(key(1)) == item(1)
        assert c.get(key(2)) == item(2)
    
    @pytest.mark.parametrize('conf', CONFS)
    def test_on_conflict(self, c, key, item, conf):
        c.put(item(1))

        c.put(item(1, val=2), on_conflict='ignore')
        assert c.get(key(1)) == item(1)

        c.put(item(1, val=3), on_conflict='replace')
        assert c.get(key(1)) == item(1, val=3)


class Test_update:
    
    @pytest.mark.parametrize('conf', CONFS)
    def test_update(self, c, key, item, conf):
        c.put(item(1))
        c.update(key(1), {'val': 2})
        assert c.get(key(1)) == item(1, **{'val': 2})


class Test_remove:
    
    @pytest.mark.parametrize('conf', CONFS)
    def test_default(self, c, key, item, conf):
        n = 1000
        c.put([item(i) for i in range(n)])

        c.remove(key(0))  # remove single
        assert len(c) == 999
        
        c.remove([key(i) for i in range(n)])  # remove multiple
        assert len(c) == 0


class Test_sync:
    
    @pytest.mark.parametrize('conf', CONFS)
    def test_default(self, c, key, item, conf):
        iter_zones = lambda: (item(i) for i in range(1, 10))
        c.sync(iter_zones(), chunk_size=5)
        assert [d['val'] for d in c.list()] == [1,2,3,4,5,6,7,8,9]

        iter_zones = lambda: (item(i) for i in range(1, 10) if i % 2 == 0)
        c.sync(iter_zones(), chunk_size=5)
        assert [d['val'] for d in c.list()] == [2,4,6,8]


class Test_option_key:
    
    def test_default_id_key_name(self, c):
        c.put({'id': '1', 'val': 1})
        assert c.get('1') == {'id': '1', 'val': 1}

        c.put({'key': '2', 'val': 2})
        assert c.get('2') == {'key': '2', 'val': 2}

        c.put({'name': '3', 'val': 3})
        assert c.get('3') == {'name': '3', 'val': 3}

    def test_custom_key(self):
        c = Collection('foo', key='uid')
        c.put({'uid': '1', 'val': 1})
        assert c.get('1') == {'uid': '1', 'val': 1}

    def test_custom_keys(self):
        c = Collection('foo', key=['uid', 'uuid'])

        c.put({'uid': '1', 'val': 1})
        assert c.get('1') == {'uid': '1', 'val': 1}

        c.put({'uuid': '2', 'val': 2})
        assert c.get('2') == {'uuid': '2', 'val': 2}


class Test_option_primary_key:
    
    pass


class Test_option_indexes:
    
    def test_composite_indexes(self):
        c = Collection('foo', **{
            'fields': {
                'name': 'str',
                'city': 'str',
                'region': 'str',
                'age': 'int',
            },
            'indexes': [
                'age',  # simple index
                ('city', 'region'),  # non unique composite index
                (('name',), True),  # unique index using peewee Meta.indexes syntax
            ],
        })

        indexes = c.model._meta.indexes
        assert indexes[0] == (('age',), False)
        assert indexes[1] == (('city', 'region'), False)
        assert indexes[2] == (('name',), True)


class Test_misc:

    def test_option_override(self):
        c = Collection('foo', on_conflict='ignore')
        assert c._opt('on_conflict') == 'ignore'
        assert c._opt('on_conflict', {'on_conflict': 'replace'}) == 'replace'
    
    def test_pure_options_constructor(self):
        c = Collection(**{
            'database': ':memory:',
            'table': 'foo',
        })
    
    def test_primary_key_specify(self):
        # no specify
        c = Collection('foo')
        assert c.model._meta.primary_key.column_name == '_key'

        # specify in field
        c = Collection('foo', **{
            'fields': {
                'name': {'type': 'str', 'primary_key': True},
                'age': {'type': 'int'},
            },
        })
        assert c.model._meta.primary_key.column_name == 'name'

        # specify separately
        c = Collection('foo', **{
            'fields': {
                'name': {'type': 'str'},
                'age': {'type': 'int'},
            },
            'primary_key': 'name',
        })
        assert c.model._meta.primary_key.column_name == 'name'

        # specify composite key
        c = Collection('foo', **{
            'fields': {
                'name': {'type': 'str'},
                'age': {'type': 'int'},
            },
            'primary_key': ['name', 'age'],
        })
        assert c.model._meta.primary_key.field_names == ('name', 'age')


def test_normalized_fields():
    fields = _set_options_defaults({})['fields']
    assert '_key' in fields
    assert '_data' in fields

    fields = _set_options_defaults({
        'fields': {
            'age': 'int',
        },
    })['fields']
    assert 'age' in fields

    fields = _set_options_defaults({
        'fields': {
            'age': {'index': True},
        },
    })['fields']
    field = fields['age']
    assert field['type'] == 'str'
    assert field['index']


@pytest.fixture
def c(request):
    if hasattr(request.node, 'callspec'):
        conf = request.node.callspec.params['conf']
        if conf['composite_key']:
            database = peewee.SqliteDatabase(':memory:')
            table = type('foo', (peewee.Model,), {
                'Meta': type('Meta', (), {
                    'primary_key': peewee.CompositeKey('id0', 'id1'),
                }),
                'id0': peewee.IntegerField(),
                'id1': peewee.FloatField(),
                'val': peewee.IntegerField(),
            })
            database.bind([table])
            database.create_tables([table])
            return Collection('foo', database)

    return Collection('foo', auto_key_type=int)


@pytest.fixture
def item(request):
    conf = request.node.callspec.params['conf']
    if conf['composite_key']:
        def item(i, **overrides):
            return {'id0': i, 'id1': float(i), 'val': i, **overrides}
    else:
        def item(i, **overrides):
            return {'id': i, 'val': i, **overrides}
    return item


@pytest.fixture
def key(request):
    conf = request.node.callspec.params['conf']
    if conf['composite_key']:
        return lambda d: (d, float(d))
    else:
        return lambda d: d


@pytest.fixture
def keys(key):
    return lambda keys: [key(d) for d in keys]
