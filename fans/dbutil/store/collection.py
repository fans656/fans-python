import json
from collections.abc import Iterable

import peewee
from fans.fn import chunked
from fans.bunch import bunch
from fans.dbutil import migrate
from fans.dbutil.introspect import models_from_database


class Collection:
    
    def __init__(self, table_name, database, *, _database_level_cache=None, **options):
        _set_options_defaults(options)

        self.table_name = table_name
        self.database = database
        self.options = options
        
        self._database_level_cache = _database_level_cache or bunch()
        
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
            row[self._auto_data_field] = json.dumps({
                k: v for k, v in item.items() if k not in self._field_names_set
            })
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
            return tuple(
                getattr(row, field_name)
                for field_name in self.model._meta.primary_key.field_names
            )
    
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

    def _derive_model(self, table_name, database):
        model = _model_from_options(self.options, table_name, database)

        if table_name in database.get_tables():
            if self._opt('_empty_schema'):
                models = _cached(lambda: models_from_database(database), self._database_level_cache, 'models')
                model = models[table_name]
            else:
                # minor todo: pass cached models to sync
                performed_actions = migrate.sync(model, database=database, droptables=False)
                if performed_actions:
                    for action in performed_actions:
                        match action['type']:
                            case 'add_column':
                                model.update(**{
                                    action['column_name']: peewee.fn.json_extract(
                                        getattr(model, self._auto_data_field),
                                        '$.age',
                                    ),
                                }).execute()
        else:
            database.bind([model])
            database.create_tables([model])

        return model

    def _keep_rows_order_with_keys(self, rows, keys):
        key_to_index = {key: index for index, key in enumerate(keys)}
        return sorted(rows, key=lambda row: key_to_index[self._get_row_key(row)])


def _model_from_options(options, table_name, database):
    body = {}
    
    for name, spec in options['fields'].items():
        body[name] = _model_field_from_field_spec(spec)

    # TODO: composite key/index

    return type(table_name, (peewee.Model,), body)


def _model_field_from_field_spec(spec):
    cls = _field_type_to_peewee_field_class(spec['type'])
    kwargs = {
        'primary_key': spec.get('primary_key', False),
        'index': spec.get('index', False),
        'null': spec.get('null', False),
    }
    return cls(**kwargs)


def _set_options_defaults(options):
    # option 'key' - item field name which will be used as key,
    #     e.g. {'id': 1, ...} -> 1 using 'id'.
    # If None then will search for 'id', 'key', 'name' in order.
    # If str then will use the given field name,
    #     e.g. {'node_id': 123, ...} -> 123 using 'node_id'.
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

    options.setdefault('auto_key_type', 'str')
    options.setdefault('auto_key_field', '__key')
    options.setdefault('auto_data_field', '__data')
    options.setdefault('primary_key', options['auto_key_field'])
    
    options['_empty_schema'] = 'fields' not in options

    options['fields'] = _normalized_fields(options)
    
    return options


def _normalized_fields(options):
    fields = options.get('fields', {})

    auto_key_field = options['auto_key_field']
    if auto_key_field is not None:
        fields[auto_key_field] = options['auto_key_type']
    
    auto_data_field = options['auto_data_field']
    if auto_data_field is not None:
        fields[auto_data_field] = 'str'
    
    primary_key = options['primary_key']
    composite_key = isinstance(primary_key, (tuple, list))
    for name, spec in fields.items():
        spec = _normalized_field_spec(name, spec)
        if not composite_key and name == primary_key:
            spec['primary_key'] = True
        fields[name] = spec

    return fields


def _normalized_field_spec(name, spec):
    ret = {'name': name}

    if isinstance(spec, (type, str)):
        spec = {'type': spec}
    elif not isinstance(spec, dict):
        raise TypeError(f'invalid field spec: {spec}')

    ret.update(spec)

    if 'type' not in ret:
        ret['type'] = 'str'
    
    if 'null' not in ret:
        ret['null'] = True

    return ret


def _field_type_to_peewee_field_class(field_type):
    if isinstance(field_type, str):
        match field_type:
            case 'str':
                return peewee.TextField
            case 'int':
                return peewee.IntegerField
            case 'float':
                return peewee.FloatField
    else:
        if field_type is str:
            return peewee.TextField
        elif field_type is int:
            return peewee.IntegerField
        elif field_type is float:
            return peewee.FloatField

    raise TypeError(f'unsupported field type {field_type}')


def _cached(make_value, cache, attr_name):
    if getattr(cache, attr_name, None) is None:
        setattr(cache, attr_name, make_value())
    return getattr(cache, attr_name)
