import json
import functools

import peewee
from fans.bunch import bunch
from fans.dbutil.introspect import models_from_database

from . import cons
from .collection import Collection


class Store:

    def __init__(self, path):
        self.path = path
        self.database = peewee.SqliteDatabase(path)
        
        table_names = set(self.database.get_tables())
        
        self.meta = self._ensure_meta(table_names)

        self._name_to_model = {}
        
        self._sync()

        self._name_to_collection = {}
        self._link_table_name_to_link_model = {}
        self._field_links = {}

    def get_collection(self, name: str = cons.DEFAULT_DOMAIN):
        if name not in self._name_to_collection:
            collection = Collection(
                name,
                **self._initialize_collection(name),
                database=self.database,
            )
            self._name_to_collection[name] = collection
            self.meta[f'doc_{name}'] = name

        return self._name_to_collection[name]

    def get_link_model(self, src_domain: str, dst_domain: str):
        link_table_name = f'__link__{src_domain}__{dst_domain}'
        if link_table_name not in self._link_table_name_to_link_model:
            Link = type(link_table_name, (peewee.Model,), {
                'Meta': type('Meta', (), {
                    'primary_key': peewee.CompositeKey('src', 'dst'),
                }),
                'src': peewee.TextField(index=True),
                'dst': peewee.TextField(index=True),
                'rel': peewee.TextField(index=True),
            })
            tables = [Link]
            self.database.bind(tables)
            self.database.create_tables(tables)
            self._link_table_name_to_link_model[link_table_name] = Link
        return self._link_table_name_to_link_model[link_table_name]

    def get_field_link(self, name):
        return self._field_links[name]

    def ensure_field_link(self, rel: str, src_domain: str, dst_domain: str):
        if rel not in self._field_links:
            self._field_links[rel] = (src_domain, dst_domain)
            self.meta[f'field_link_{rel}'] = [src_domain, dst_domain]
        assert self._field_links[rel] == (src_domain, dst_domain)
    
    @functools.cached_property
    def _existing_models(self):
        return models_from_database(self.database)

    def _get_meta_table(self):
        Meta = type('nos_meta', (peewee.Model,), {
            'key': peewee.TextField(primary_key=True),
            'value': peewee.TextField(),
        })
        tables = [Meta]
        self.database.bind(tables)
        self.database.create_tables(tables)
        return Meta
    
    def _ensure_meta(self, table_names):
        if cons.META_TABLE_NAME not in table_names:
            model = self._create_meta_table()
        else:
            model = self._get_meta_model()
        
        meta = Meta(model)
        
        return meta
    
    def _create_meta_table(self) -> 'peewee.Model':
        Meta = self._get_meta_model()
        self.database.create_tables([Meta])
        return Meta
    
    def _get_meta_model(self) -> 'peewee.Model':
        model = type(cons.META_TABLE_NAME, (peewee.Model,), {
            'key': peewee.TextField(primary_key=True),
            'value': peewee.TextField(),
        })
        self.database.bind([model])
        return model
    
    def _sync(self):
        self._name_to_model.update(models_from_database(self.database))
        
        print(self._name_to_model)
    
    def _initialize_collection(self, name):
        Item = type(name, (peewee.Model,), {
            'id': peewee.TextField(primary_key=True),
            'data': peewee.TextField(),
        })
        Label = type(f'__label__{name}', (peewee.Model,), {
            'Meta': type('Meta', (), {
                'primary_key': peewee.CompositeKey('item_id', 'label_key', 'label_value'),
            }),
            'item_id': peewee.TextField(index=True),
            'label_key': peewee.TextField(index=True),
            'label_value': peewee.TextField(index=True),
        })
        tables = [Item, Label]

        self.database.bind(tables)
        self.database.create_tables(tables)
        
        return bunch(Item=Item, Label=Label)


class Meta:
    
    def __init__(self, model):
        self.__model = model
    
    def __getitem__(self, key):
        M = self.__model
        return next((json.loads(d.value) for d in M.select(M.value).where(M.key == key)), None)
    
    def __setitem__(self, key, value):
        self.__model.insert(key=key, value=json.dumps(value)).on_conflict_replace().execute()
    
    def __iter__(self):
        return iter((d.key, json.loads(d.value)) for d in self.__model.select())
    
    def __repr__(self):
        return repr(dict(self))
