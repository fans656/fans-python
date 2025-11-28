import pytest
import peewee

from fans.dbutil.store.collection import Collection


CONFS = [
    {'composite_key': False},
    {'composite_key': True},
]


def test_usage_basic():
    c = Collection('foo', peewee.SqliteDatabase(':memory:'))

    # put
    c.put({'name': 'foo', 'age': 3})
    c.put({'name': 'bar', 'age': 5})
    
    # get
    assert c.get('foo') == {'name': 'foo', 'age': 3}
    assert c.get('bar') == {'name': 'bar', 'age': 5}

    # count
    assert c.count() == 2

    # remove
    c.remove('foo')
    assert c.count() == 1


def test_usage_from_existing_database():
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


def test_usage_auto_migration():
    database = peewee.SqliteDatabase(':memory:')

    c = Collection('foo', database)
    c.put({'name': 'foo', 'age': 3})

    # schema updated: age as separate column using index
    c = Collection('foo', database)
    c.put({'name': 'bar', 'age': 5})


class Test_get:
    
    @pytest.mark.parametrize('conf', CONFS)
    def test_default(self, c, key, keys, item, conf):
        c.put(item(1))
        assert c.get(key(1)) == item(1)  # get single

        c.put(item(2))
        assert c.get(keys([1, 2])) == [item(1), item(2)]  # get multiple
        assert c.get(keys([2, 1])) == [item(2), item(1)]  # same order as given keys


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


class Test_remove:
    
    @pytest.mark.parametrize('conf', CONFS)
    def test_default(self, c, key, item, conf):
        n = 1000
        c.put([item(i) for i in range(n)])

        c.remove(key(0))  # remove single
        assert len(c) == 999
        
        c.remove([key(i) for i in range(n)])  # remove multiple
        assert len(c) == 0


class Test_option_key:
    
    def test_default_id_key_name(self, c):
        c.put({'id': '1', 'val': 1})
        assert c.get('1') == {'id': '1', 'val': 1}

        c.put({'key': '2', 'val': 2})
        assert c.get('2') == {'key': '2', 'val': 2}

        c.put({'name': '3', 'val': 3})
        assert c.get('3') == {'name': '3', 'val': 3}

    def test_custom_key(self):
        c = Collection('foo', peewee.SqliteDatabase(':memory:'), key='uid')
        c.put({'uid': '1', 'val': 1})
        assert c.get('1') == {'uid': '1', 'val': 1}

    def test_custom_keys(self):
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

    return Collection('foo', peewee.SqliteDatabase(':memory:'), auto_key_type=peewee.IntegerField)


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
