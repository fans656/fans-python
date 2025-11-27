import peewee

from .collection import Collection


class Store:
    
    def __init__(self, arg: str|peewee.Database):
        if isinstance(arg, peewee.Database):
            self.database = arg
        else:
            self.database = peewee.SqliteDatabase(arg)
        
        self._name_to_collection = {}
    
    def get_collection(self, name: str) -> Collection:
        collection = self._name_to_collection.get(name)
        if not collection:
            collection = self._name_to_collection[name] = Collection(
                name,
                database=self.database,
            )
        return collection
