import functools

from fans.dbutil.store import Store
from fans.dbutil.store.collection import Collection
from fans.dbutil.tagging import tagging


class Nos:
    
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('collection_class', EnhancedCollection)
        self.store = Store(*args, **kwargs)
    
    def get(self, *args, collection: str = 'default', **kwargs):
        c = self.store.get_collection(collection)
        return c.get(*args, **kwargs)
    
    def put(self, *args, collection: str = 'default', **kwargs):
        c = self.store.get_collection(collection)
        return c.put(*args, **kwargs)
    
    def count(self, *args, collection: str = 'default', **kwargs):
        c = self.store.get_collection(collection)
        return c.count(*args, **kwargs)
    
    def collection(self, name: str):
        return self.store.get_collection(name)


class EnhancedCollection(Collection):
    
    def tag(self, *args, **kwargs):
        self.tagging.add_tag(*args, **kwargs)
    
    def untag(self, *args, **kwargs):
        self.tagging.remove_tag(*args, **kwargs)
    
    def find(self, *args, **kwargs):
        kwargs['return_query'] = True
        query = self.tagging.find(*args, **kwargs)
        return self.get(lambda m: m.select().where(m._meta.primary_key << query))
    
    def tags(self):
        return self.tagging.tags()
    
    @functools.cached_property
    def tagging(self):
        return tagging(self.database, f'{self.table_name}_tag', target=self.model)
