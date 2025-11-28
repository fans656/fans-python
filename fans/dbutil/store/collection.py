import json
from collections.abc import Iterable

import peewee
from fans.fn import chunked


class Collection:
    
    def __init__(self, table_name, database, **options):
        # option 'key' - item field name which will be used as key,
        # e.g. {'id': 1, ...} -> 1 using 'id'.
        # If None then will search for 'id', 'key', 'name' in order.
        # If str then will use the given field name,
        # e.g. {'node_id': 123, ...} -> 123 using 'node_id'.
        # If list[str] then will use the given field names to search in order.
        options.setdefault('key', ['id', 'key', 'name'])
        if not isinstance(options['key'], (tuple, list)):
            options['key'] = [options['key']]

        # whether convert item to dict when returning item from `get` etc
        options.setdefault('raw', False)

        # chunk size when putting multiple items
        options.setdefault('chunk_size', 500)

        # conflict behavior when putting
        options.setdefault('on_conflict', 'replace')

        # order behavior when get multiple items
        options.setdefault('order', 'keep')

        options.setdefault('auto_key_type', peewee.TextField)
        options.setdefault('auto_key_field', '__key')
        options.setdefault('auto_data_field', '__data')

        self.table_name = table_name
        self.database = database
        self.options = options
        
        self._key_fields = self._opt('key')
        self._auto_key_field = self._opt('auto_key_field')
        self._auto_data_field = self._opt('auto_data_field')
        
        self.model = self._derive_model(table_name, database)
        
        meta = self.model._meta

        self._field_names = [d for d in meta.fields if not d.startswith('_')]
        self._field_names_set = set(self._field_names)
        self._has_auto_key_field = self._auto_key_field in meta.fields
        self._has_auto_data_field = self._auto_data_field in meta.fields
    
    def get(self, arg, **options):
        if isinstance(arg, list):
            keys = arg
            query = self.model.select().where(self.model._meta.primary_key << keys)
            rows = query
            if self._opt('order', options) == 'keep':
                rows = self._keep_rows_order_with_keys(rows, keys)
            return [self._row_to_item(row, options) for row in rows]
        elif callable(arg):
            prepare_query = arg
            query = prepare_query(self.model)
            return query if self._opt('raw', options) else map(self._row_to_item, query)
        else:
            key = arg
            row = self.model.get_or_none(self.model._meta.primary_key == key)
            return self._row_to_item(row, options)
    
    def put(self, item_or_items, **options):
        if isinstance(item_or_items, dict):
            items = [item_or_items]
        elif isinstance(item_or_items, Iterable):
            items = item_or_items
        else:
            raise TypeError(f'unknown item(s) of type {type(item_or_items)}')
        
        rows = (self._item_to_row(item, options) for item in items)

        on_conflict = self._on_conflict(options)
        for _rows in chunked(rows, self._opt('chunk_size', options)):
            on_conflict(self.model.insert_many(_rows)).execute()
    
    def remove(self, key_or_keys, **options):
        if isinstance(key_or_keys, list):
            keys = key_or_keys
        else:
            keys = [key_or_keys]
        for _keys in chunked(keys, self._opt('chunk_size', options)):
            self.model.delete().where(self.model._meta.primary_key << _keys).execute()
    
    def list(self, **options):
        return list(self.iter(**options))
    
    def iter(self, **options):
        for row in self.model.select():
            yield self._row_to_item(row, options)
    
    def count(self):
        return self.model.select().count()
    
    def find_by_tag(self, query: str):
        pass
    
    def __len__(self):
        return self.count()
    
    def __iter__(self):
        return self.iter()
    
    def _opt(self, name, options={}):
        return options.get(name, self.options.get(name))
    
    def _item_to_row(self, item, options):
        row = {}
        if self._has_auto_key_field:
            row[self._auto_key_field] = self._get_item_key(item)
        for field_name in self._field_names:
            row[field_name] = item.get(field_name)
        if self._has_auto_data_field:
            row[self._auto_data_field] = json.dumps({k: v for k, v in item.items() if k not in self._field_names_set})
        return row
    
    def _row_to_item(self, row, options):
        if row is None:
            return row
        if self._opt('raw', options):
            return row
        ret = {
            field_name: getattr(row, field_name)
            for field_name in self._field_names
        }
        if self._has_auto_data_field:
            ret.update(json.loads(getattr(row, self._auto_data_field)))
        return ret
    
    def _get_row_key(self, row):
        if self._has_auto_key_field:
            return getattr(row, self._auto_key_field)
        else:
            raise NotImplementedError()
    
    def _get_item_key(self, item):
        for key_field in self._key_fields:
            key = item.get(key_field)
            if key is not None:
                return key
        else:
            raise ValueError(f'no usable key: {item}')
    
    def _on_conflict(self, options):
        on_conflict = self._opt('on_conflict', options)
        match on_conflict:
            case 'replace':
                return lambda query: query.on_conflict_replace()
            case 'ignore':
                return lambda query: query.on_conflict_ignore()
            case _:
                raise ValueError(f'invalid on_conflict behavior "{on_conflict}"')

    def _derive_model(self, name, database):
        if name in database.get_tables():
            raise NotImplementedError()
        else:
            model = type(name, (peewee.Model,), {
                self._auto_key_field: self._opt('auto_key_type')(primary_key=True),
                self._auto_data_field: peewee.TextField(),
            })
            database.bind([model])
            database.create_tables([model])
        return model

    def _keep_rows_order_with_keys(self, rows, keys):
        key_to_index = {key: index for index, key in enumerate(keys)}
        return sorted(rows, key=lambda row: key_to_index[self._get_row_key(row)])
