import peewee

from fans.db.migrate import sync, Model


def test_create_database(tmp_path):
    database_path = tmp_path / 't.sqlite'
    database = peewee.SqliteDatabase(database_path)
    model = type('Foo', (peewee.Model,), {})
    model.bind(database)
    sync(model)
    assert database.table_exists('foo')

    database = peewee.SqliteDatabase(':memory:')
    model = type('Foo', (peewee.Model,), {})
    model.bind(database)
    sync(model)
    assert database.table_exists('foo')


def test_delete_table():
    database = peewee.SqliteDatabase(':memory:')
    base = type('Base', (peewee.Model,), {'Meta': type('Meta', (), {'database': database})})
    foo = type('Foo', (base,), {})
    bar = type('Bar', (base,), {})
    database.create_tables([foo, bar])

    sync(foo)

    assert database.table_exists('foo')
    assert not database.table_exists('bar')


def test_rename_table():
    database = peewee.SqliteDatabase(':memory:')
    base = type('Base', (peewee.Model,), {'Meta': type('Meta', (), {'database': database})})
    src = type('Foo', (base,), {})
    dst = type('Bar', (base,), {})
    database.create_tables([src])

    sync((dst, [('Foo', 'Bar')]))

    assert not database.table_exists('foo')
    assert database.table_exists('bar')


def test_rename_column():
    database = peewee.SqliteDatabase(':memory:')
    base = type('Base', (peewee.Model,), {'Meta': type('Meta', (), {'database': database})})
    src = type('Foo', (base,), {'one': peewee.TextField()})
    dst = type('Foo', (base,), {'two': peewee.TextField()})
    database.create_tables([src])

    sync((dst, [('one', 'two')]))

    names = {col.name for col in database.get_columns('foo')}
    assert 'one' not in names


def test_change_primary_key():
    database = peewee.SqliteDatabase(':memory:')
    base = type('Base', (peewee.Model,), {'Meta': type('Meta', (), {'database': database})})
    src = type('Foo', (base,), {})
    dst = type('Foo', (base,), {'code': peewee.TextField(primary_key = True)})
    database.create_tables([src])
    sync(dst)
    assert database.get_primary_keys('foo') == ['code']

    database = peewee.SqliteDatabase(':memory:')
    src = type('Foo', (peewee.Model,), {})
    dst = type('Foo', (peewee.Model,), {
        'Meta': type('Meta', (), {
            'primary_key': peewee.CompositeKey('code', 'name'),
        }),
        'code': peewee.TextField(),
        'name': peewee.TextField(),
    })
    database.bind([src, dst])
    database.create_tables([src])
    sync(dst)
    assert database.get_primary_keys('foo') == ['code', 'name']


def test_add_columns():
    database = peewee.SqliteDatabase(':memory:')
    src = type('Foo', (peewee.Model,), {})
    dst = type('Foo', (peewee.Model,), {'code': peewee.TextField(null = True)})
    database.bind([src, dst])
    database.create_tables([src])
    sync(dst)
    dst.insert({'code': '000001'}).execute()
    assert dst.select().get().code == '000001'


def test_del_columns():
    database = peewee.SqliteDatabase(':memory:')
    src = type('Foo', (peewee.Model,), {'code': peewee.TextField()})
    dst = type('Foo', (peewee.Model,), {})
    database.bind([src, dst])
    database.create_tables([src])
    sync(dst)
    assert [col.name for col in database.get_columns('foo')] == ['id']


def test_add_indexes():
    database = peewee.SqliteDatabase(':memory:')
    src = type('Foo', (peewee.Model,), {
        'code': peewee.TextField(index = True),
        'name': peewee.TextField(),
    })
    dst = type('Foo', (peewee.Model,), {
        'code': peewee.TextField(index = True),
        'name': peewee.TextField(index = True),
        'Meta': type('Meta', (), {
            'indexes': [
                (('code', 'name'), False),
            ],
        }),
    })
    database.bind([src, dst])
    database.create_tables([src])
    sync(dst)
    indexes = {tuple(index.columns) for index in database.get_indexes('foo')}
    assert ('code',) in indexes
    assert ('name',) in indexes
    assert ('code', 'name') in indexes


def test_del_indexes():
    database = peewee.SqliteDatabase(':memory:')
    src = type('Foo', (peewee.Model,), {
        'code': peewee.TextField(index = True),
        'name': peewee.TextField(),
        'Meta': type('Meta', (), {
            'indexes': [
                (('code', 'name'), False),
            ],
        }),
    })
    dst = type('Foo', (peewee.Model,), {
        'code': peewee.TextField(),
        'name': peewee.TextField(index = True),
    })
    database.bind([src, dst])
    database.create_tables([src])
    sync(dst)
    indexes = {tuple(index.columns) for index in database.get_indexes('foo')}
    assert ('code',) not in indexes
    assert ('name',) in indexes
    assert ('code', 'name') not in indexes
