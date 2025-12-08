import functools

from fans.dbutil.store import Store
from fans.dbutil.store.collection import Collection
from fans.dbutil.tagging import tagging


def _delegated(method_name):
    def func(self, *args, collection: str = 'default', **kwargs):
        c = self.store.get_collection(collection)
        return getattr(c, method_name)(*args, **kwargs)
    func.__name__ = method_name
    return func


class Nos:
    
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('collection_class', EnhancedCollection)
        self.store = Store(*args, **kwargs)
    
    def collection(self, name: str):
        return self.store.get_collection(name)
    
    get = _delegated('get')
    put = _delegated('put')
    update = _delegated('update')
    remove = _delegated('remove')
    count = _delegated('count')
    list = _delegated('list')


class EnhancedCollection(Collection):
    
    def add_tag(self, *args, **kwargs):
        self.tagging.add_tag(*args, **kwargs)
    
    def remove_tag(self, *args, **kwargs):
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
