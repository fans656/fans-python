import peewee

from . import cons
from .collection import Collection


class Store:

    def __init__(self, path):
        self.path = path
        self.database = peewee.SqliteDatabase(path)

        self.Meta = self._get_meta_table()
        self._name_to_collection = {}
        self._link_table_name_to_link_model = {}
        self._field_links = {}

    def get_collection(self, name: str = cons.DEFAULT_DOMAIN):
        if name not in self._name_to_collection:
            collection = Collection(name, self.database)
            collection.initialize()
            self._name_to_collection[name] = collection
            self._update_meta(f'doc_{name}', name)

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
            self._update_meta(f'field_link_{rel}', [src_domain, dst_domain])
        assert self._field_links[rel] == (src_domain, dst_domain)

    def _get_meta_table(self):
        Meta = type('nos_meta', (peewee.Model,), {
            'key': peewee.TextField(primary_key=True),
            'value': peewee.TextField(),
        })
        tables = [Meta]
        self.database.bind(tables)
        self.database.create_tables(tables)
        return Meta

    def _update_meta(self, key, value):
        self.Meta.insert(key=key, value=value).on_conflict_replace().execute()
