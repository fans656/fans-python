import peewee

from fans.dbutil.introspect import models_from_database
from fans.nos import nos


def test_use_existing_database(tmp_path):
    print('=' * 80)
    database_path = tmp_path / 'data.sqlite'
    database = peewee.SqliteDatabase(database_path)
    
    class Person(peewee.Model):
        
        uid = peewee.TextField(primary_key=True)
        name = peewee.TextField()
        age = peewee.IntegerField()
    
    tables = [Person]
    database.bind(tables)
    database.create_tables(tables)
    
    Person.insert_many([
        {'uid': '123', 'name': 'foo', 'age': 3},
        {'uid': '456', 'name': 'bar', 'age': 5},
    ]).execute()

    store = nos(database_path)
    persons = store.collection('person')
    return
    for d in persons:
        print(d)
