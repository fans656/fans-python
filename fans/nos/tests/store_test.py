import peewee

from fans.nos import cons
from fans.nos.store import Store


class Test_ensure_meta:

    def test_new(self):
        store = Store(':memory:')
        assert cons.META_TABLE_NAME in store.database.get_tables()
    
    def test_existing(self, tmp_path):
        database_path = tmp_path / 'data.sqlite'
        database = peewee.SqliteDatabase(database_path)
        
        class Person(peewee.Model):
            
            uid = peewee.TextField(primary_key=True)
            name = peewee.TextField()
            age = peewee.IntegerField()
        
        tables = [Person]
        database.bind(tables)
        database.create_tables(tables)

        store = Store(database_path)
        assert cons.META_TABLE_NAME in store.database.get_tables()
