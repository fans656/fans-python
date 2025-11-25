import peewee

from fans.dbutil import introspect


def test_composite_key():
    database = peewee.SqliteDatabase(':memory:')
    
    class Person(peewee.Model):
        
        class Meta:
            
            primary_key = peewee.CompositeKey('first_name', 'last_name')
        
        first_name = peewee.TextField()
        last_name = peewee.TextField()
        age = peewee.IntegerField()
    
    database.bind([Person])
    database.create_tables([Person])
    
    items = [
        {'first_name': 'Alex', 'last_name': 'Honnold', 'age': 40},
        {'first_name': 'Bob', 'last_name': 'Dylan', 'age': 84},
    ]
    
    Person.insert_many(items).execute()
    
    _Person = introspect.models_from_database(database)['person']

    database.bind([_Person])
    assert _Person.select().dicts() == items
