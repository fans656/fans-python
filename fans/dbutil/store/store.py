import peewee
from fans.bunch import bunch

from .collection import Collection


class Store:
    
    def __init__(self, arg: str|peewee.Database):
        if isinstance(arg, peewee.Database):
            self.database = arg
        else:
            self.database = peewee.SqliteDatabase(arg)
        
        self._name_to_collection = {}
        self._database_level_cache = bunch()
    
    def get_collection(self, name: str) -> Collection:
        collection = self._name_to_collection.get(name)
        if collection is None:
            collection = self._name_to_collection[name] = Collection(
                name,
                database=self.database,
                _database_level_cache=self._database_level_cache,
            )
        return collection
