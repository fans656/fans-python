import peewee

from fans import dbutil


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

    tagging.add_tag([0, 2, 4, 6, 8], 'even')
    tagging.add_tag([1, 3, 5, 7, 9], 'odd')
    tagging.add_tag([2, 3, 5, 7], 'prime')
    tagging.add_tag([0, 1, 4, 9], 'square')
    tagging.add_tag([0, 1, 8], 'cube')
    tagging.add_tag(6, 'perfect')
    tagging.add_tag([1, 2, 6], 'factorial')

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


def test_specify_table_name():
    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db)
    assert 'tag' in set(db.get_tables())

    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db, 'foo')
    assert 'foo' in set(db.get_tables())
    assert 'tag' not in set(db.get_tables())


def test_get_all_tags():
    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db)
    tagging.add_tag(1, 'foo')
    tagging.add_tag(1, 'bar')
    tagging.add_tag(2, 'bar')
    tagging.add_tag(2, 'baz')
    tags = tagging.tags()
    assert len(tags) == 3
    assert set(tags) == {'foo', 'bar', 'baz'}


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
    tagging.add_tag(1, 'foo')
    tagging.add_tag(1, 'bar')
    tagging.add_tag(2, 'bar')
    tagging.add_tag(2, 'baz')

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
    tagging = dbutil.tagging(db, key_type=str)
    assert list(db.execute_sql('pragma table_info(tag)'))[0][2] == 'TEXT'

    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db, key_type=float)
    assert list(db.execute_sql('pragma table_info(tag)'))[0][2] == 'REAL'


def test_composite_key():
    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db, key_type=(float, str))

    tagging.add_tag((1.5, 'foo'), 'red')
    tagging.add_tag((1.5, 'bar'), 'red')
    tagging.add_tag((3.0, 'baz'), 'blue')

    assert set(tagging.find('red')) == {(1.5, 'foo'), (1.5, 'bar')}
    assert set(tagging.find('blue')) == {(3.0, 'baz')}

    assert set(tagging.tags()) == {'red', 'blue'}


def test_batch_tagging():
    db = peewee.SqliteDatabase(':memory:')
    tagging = dbutil.tagging(db)
    tagging.add_tag([
        (1, 'foo'),
        (1, 'bar'),
        (2, 'foo'),
    ])
    assert set(tagging.find('foo')) == {1, 2}
    assert set(tagging.find('bar')) == {1}
