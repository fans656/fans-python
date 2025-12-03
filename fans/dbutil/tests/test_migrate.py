#import pytest
#import peewee
#
#from fans.dbutil import migrate
#
#
#class Foo(peewee.Model):
#
#    name = peewee.TextField(primary_key=True)
#    age = peewee.IntegerField()
#
#
#class Bar(peewee.Model):
#
#    uid = peewee.TextField(primary_key=True)
#    val = peewee.IntegerField()
#
#
#class Test_utils:
#    
#    def test_add_table_drop_table(self, database):
#        class Person(peewee.Model):
#            name = peewee.TextField(primary_key=True)
#            age = peewee.IntegerField()
#
#        migrate.add_table(database, Person)
#        assert database.get_tables() == ['person']
#        
#        migrate.drop_table(database, 'person')
#        assert database.get_tables() == []
#
#
#class Test_migration:
#    
#    def test_usage_create_tables_and_drop_tables(self, database):
#        migration = migrate.Migration(database, [Foo, Bar])
#        migration.execute()
#        assert database.get_tables() == ['bar', 'foo']  # created
#        
#        migration = migrate.Migration(database, [Foo])
#        migration.execute()
#        assert database.get_tables() == ['foo']  # bar dropped
#    
#    def test_actions_and_dryrun(self, database):
#        migration = migrate.Migration(database, [Foo])
#
#        actions = migration.execute(dryrun=True)
#        assert len(actions) == 1
#        assert actions[0]['type'] == 'add_table'
#        assert database.get_tables() == []
#
#        migration.execute()
#        assert database.get_tables() == ['foo']
#
#
#@pytest.fixture
#def database():
#    return peewee.SqliteDatabase(':memory:')
