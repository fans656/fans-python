import random

import peewee

from . import sqlite_sync as M


def test_default(tmp_path):
    
    class Foo(peewee.Model):

        name = peewee.TextField(primary_key=True)
        age = peewee.IntegerField()
        added = peewee.IntegerField()
    
    class Bar(peewee.Model):

        key = peewee.TextField(primary_key=True)
        value = peewee.TextField()
    
    database_path = tmp_path / 'db.sqlite'
    database = peewee.SqliteDatabase(database_path)
    tables = [Foo, Bar]
    database.bind(tables)
    database.create_tables(tables)
    
    Foo.insert_many([{
        'name': f'item-{i + 1:03}',
        'age': random.randint(0, 100),
        'added': i + 1,
    } for i in range(10)]).execute()
    
    ################################################################################
    
    count, cursor = M.get_items_later_than(
        str(database_path),
        'foo',
        when=8,
        #fields=[],
    )
    print('count', count)
    
    dumpped = M.dump_items(cursor, threshold=128)
    print(dumpped)
    print('-' * 80)
    for item in M.load_items(dumpped):
        print(item)
    
    #print()
    #for table_name in database.get_tables():
    #    print('---', table_name)
    #    for column in database.get_columns(table_name):
    #        print(column)
