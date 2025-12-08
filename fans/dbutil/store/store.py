import peewee
from fans.bunch import bunch

from .collection import Collection


class Store:
    
    def __init__(
        self,
        path: str|peewee.Database = ':memory:',
        collection_class=Collection,
        **options,
    ):
        if isinstance(path, peewee.Database):
            self.database = path
        else:
            self.database = peewee.SqliteDatabase(path)
        
        self._name_to_collection_options = options.pop('collections', {})
        
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
                **{
                    **self.options,
                    **self._name_to_collection_options.get(name, {}),
                    **options,
                },
            )
        return collection
