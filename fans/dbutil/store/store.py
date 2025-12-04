import peewee
from fans.bunch import bunch

from .collection import Collection


class Store:
    
    def __init__(
        self,
        arg: str|peewee.Database = ':memory:',
        collection_class=Collection,
        **options,
    ):
        if isinstance(arg, peewee.Database):
            self.database = arg
        else:
            self.database = peewee.SqliteDatabase(arg)
        
        self.collection_class = collection_class
        self.options = options
        
        self._name_to_collection = {}
        self._database_level_cache = bunch()
    
    def get_collection(self, name: str, **options) -> Collection:
        collection = self._name_to_collection.get(name)
        if collection is None:
            collection = self._name_to_collection[name] = self.collection_class(
                name,
                self.database,
                _database_level_cache=self._database_level_cache,
                **{**self.options, **options},
            )
        return collection
