"""
Manage a database table as a collection of items:

    c = Collection('person', peewee.SqliteDatabase(':memory:'))

    # put items into collection
    c.put({'name': 'foo', 'age': 3})  # by default use 'id'/'key'/'name' as item key
    c.put({'name': 'bar', 'age': 5})

    # get item
    assert c.get('foo') == {'name': 'foo', 'age': 3}

    # update existing item
    c.update('foo', {'age': 7})
    
    # count items
    assert c.count() == 2

    # remove item
    c.remove('bar')

by default item data is JSON serialized into `__data` column:

    _key    _data
    foo     {"name":"foo","age":7}
    bar     {"name":"bar","age":5}

you can specify fields to be in separate column:

    c = Collection('person', database, **{
        'fields': {
            'age': {'type': 'int', 'index': True},
        },
    })

    # _key    age     _data
    # foo     7       {"name":"foo"}
    # bar     5       {"name":"bar"}

or specify primary key:

    c = Collection('clip', database, **{
        'fields': {
            'node_id': 'int',
            'time_pos': 'float',
        },
        'primary_key': ['node_id', 'time_pos'],
    })
    
    c.put({'node_id': 123, 'time_pos': 60.0, 'tagging': 'thumb'})
    c.put({'node_id': 456, 'time_pos': 10.0, 'rating': 5})

    # node_id     time_pos    _data
    # 123         60.0        {'tagging': 'thumb'}
    # 456         10.0        {'rating': 5}

    c.get(123) == {'node_id': 123, 'time_pos': 60.0, 'tagging': 'thumb'}

or just use existing database table:
    
    # "person" table
    #
    # forename    surename    where
    # Alex        Honnold     mountain
    # Moby        Dick        ocean
    
    c = Collection('person', database)

    c.get(('Alex', 'Honnold')) == {'forename': 'Alex', 'surename': 'Honnold', 'where': 'mountain'}

# Note 

`Collection` is constructed by given a table name and database:

    c = Collection('person', peewee.SqliteDatabase(':memory:'))

    c = Collection(**{'table': 'person', 'database': ':memory:'})  # pure options form

see `_set_options_defaults` for all options.

Following are common methods of `Collection`:
    
    c.get(...)      # get item(s)
    c.put(...)      # put item(s)
    c.update(...)   # update item
    c.remove(...)   # remove item(s)
    c.count()       # count existing items
    c.iter()        # iterate existing items
    c.list()        # get existing items as list

see doc of each method for details.

You can always access `c.model: peewee.Model` for advanced query.

# Auto migration

When you change collection schema, by default the underlying table will be auto migrated:
    
    c = Collection('person', database)
    c.put({'name': 'foo', 'age': 3})
    
    # later changed to
    c = Collection('person', database, fields={'age': {'type': 'int', 'index': True}})

    # then table changed from:
    #
    # _key    _data
    # foo     {"name":"foo","age":3}

    # to:
    #
    # _key    _data             age     
    # foo     {"name":"foo"}    3       
"""
import json
import uuid
import itertools
import functools
from collections.abc import Iterable

import peewee
from fans.fn import chunked
from fans.bunch import bunch
from fans.dbutil import migrate
from fans.dbutil.introspect import models_from_database


class Collection:
    
    def __init__(
        self,
        table_: str = None,
        database_: str|peewee.Database = None,
        *,
        _database_level_cache=None,
        **options,
    ):
        _set_options_defaults(options, table_name=table_, database=database_)
        
        database = database_ or options['database']
        if isinstance(database, peewee.Database):
            self.database = database
        else:
            self.database = peewee.SqliteDatabase(database)

        self.table_name = table_ or options['table']
        self.options = options
        
        self._database_level_cache = _database_level_cache or bunch()
        
        self._key_fields = self._opt('key')
        self._auto_key_field = self._opt('auto_key_field')
        self._auto_data_field = self._opt('auto_data_field')
        
        self.model = self._derive_model(self.table_name, self.database)
        
        meta = self.model._meta
        
        self.is_composite_key = isinstance(meta.primary_key, peewee.CompositeKey)

        self._field_names = [d for d in meta.fields if not d.startswith('_')]
        self._field_names_set = set(self._field_names)
        self._has_auto_key_field = self._auto_key_field in meta.fields
        self._has_auto_data_field = self._auto_data_field in meta.fields
    
    def get(self, arg, **options):
        """
        Get item or items.
        
        Args:
            arg -
                if key (str|int|float|tuple), consider as single key for getting single item.
                if list, consider as multiple keys for getting multiple items.
                if callable, consider as query builder for taking c.model and return a query.
            options -
                order - for list getting, if 'keep' (default) then ensure returned items kept same order
                        as given keys
                raw - if True (default False) then don't convert item into dict
        
        For example, given collection:

            c = Collection('person')
            c.put({'name': 'foo', 'age': 3})
            c.put({'name': 'bar', 'age': 5})

        Get single item by key:
        
            c.get('foo') == {'name': 'foo', 'age': 3}
        
        Get multiple items by keys:
        
            c.get(['foo', 'bar']) == [
                {'name': 'foo', 'age': 3},
                {'name': 'bar', 'age': 5},
            ]
        
        Get by query builder callable:
        
            c = Collection('person', indexes=['age'])
            next(c.get(lambda m: m.select().where(m.age > 4))) == {'name': 'bar', 'age': 5}
        """
        if isinstance(arg, list):
            keys = arg
            query = self.model.select().where(self.model._meta.primary_key << keys)
            if self._opt('raw', options):
                return query
            rows = query
            if self._opt('order', options) == 'keep':
                rows = self._keep_rows_order_with_keys(rows, keys)
            return [self._row_to_item(row, options) for row in rows]
        elif callable(arg):
            prepare_query = arg
            query = prepare_query(self.model)
            if isinstance(query, peewee.Expression):
                query = self.model.select().where(query)
            if self._opt('raw', options):
                return query
            return [self._row_to_item(row, options) for row in query]
        else:
            key = arg
            row = self.model.get_or_none(self.model._meta.primary_key == key)
            return self._row_to_item(row, options)
    
    def put(self, item_or_items, **options):
        """
        Put item or items into collection.
        
        Args:
            item_or_items -
                if dict, consider as item.
                if iterable, consdier as items.
            options -
                chunk_size - when putting huge number of items,
                    split into chunks for each database operation, default 500.
                on_conflict - 'replace' (default) | 'ignore'
                    conflict behavior when item key already exists.
        """
        if isinstance(item_or_items, dict):
            items = [item_or_items]
        elif isinstance(item_or_items, Iterable):
            items = item_or_items
        else:
            raise TypeError(f'unknown item(s) of type {type(item_or_items)}')
        
        rows = (self._item_to_row(item) for item in items)

        on_conflict = self._on_conflict(options)
        for _rows in chunked(rows, self._opt('chunk_size', options)):
            on_conflict(self.model.insert_many(_rows)).execute()
    
    def update(self, key, update: dict, **options):
        field_update = {}
        data_update = {}
        for k, v in update.items():
            if k in self._field_names:
                field_update[k] = v
            else:
                data_update[k] = v
        if self._auto_data_field and data_update:
            field_update[self._auto_data_field] = json.dumps({**self._get_data(key), **data_update})
        self.model.update(field_update).where(self.model._meta.primary_key == key).execute()
    
    def remove(self, key_or_keys, **options):
        if isinstance(key_or_keys, list):
            keys = key_or_keys
        else:
            keys = [key_or_keys]
        for _keys in chunked(keys, self._opt('chunk_size', options)):
            self.model.delete().where(self.model._meta.primary_key << _keys).execute()
    
    def count(self):
        return self.model.select().count()
    
    def iter(self, **options):
        for row in self.model.select():
            yield self._row_to_item(row, options)
    
    def list(self, **options):
        return list(self.iter(**options))
    
    def sync(self, items: Iterable[dict], **options):
        """
        Sync collection from source of iterable items.
        
        - Existed items not in source will be removed
        """
        latest_key_table_name = f'{self.table_name}_latest_{uuid.uuid4().hex}'
        primary_key = self.model._meta.primary_key
        Meta = type('Meta', (), {
            'database': self.database,
            'temporary': True,
        })
        if isinstance(primary_key, peewee.CompositeKey):
            body = {'Meta': Meta}
            for field_name in primary_key.field_names:
                body[field_name] = getattr(self.model, field_name).__class__()
            body['primary_key'] = peewee.CompositeKey(*primary_key.field_names)
            Latest = type(latest_key_table_name, (peewee.Model,), body)
        else:
            Latest = type(latest_key_table_name, (peewee.Model,), {
                'Meta': Meta,
                'key': primary_key.__class__(primary_key=True),
            })
        Latest.create_table()

        model = self.model
        on_conflict = self._on_conflict(options)
        item_to_row = self._item_to_row
        get_row_key = self._get_row_key
        getter = lambda d, attr_name: d.get(attr_name)
        if isinstance(primary_key, peewee.CompositeKey):
            for _items in chunked(items, self._opt('chunk_size', options)):
                rows = map(item_to_row, _items)
                rows_for_key, rows_for_insert = itertools.tee(rows)
                Latest.insert_many((get_row_key(d, getter) for d in rows_for_key)).execute()
                on_conflict(model.insert_many(rows_for_insert)).execute()
            model.delete().where(
                self._primary_key.not_in(
                    Latest.select(*(
                        getattr(Latest, field_name) for field_name in primary_key.field_names
                    ))
                )
            ).execute()
        else:
            _get_item_key = self._get_item_key
            _item_to_row = self._item_to_row
            for _items in chunked(items, self._opt('chunk_size', options)):
                _items_for_key, _items_for_item = itertools.tee(_items)
                Latest.insert_many(((_get_item_key(d),) for d in _items_for_key)).execute()
                on_conflict(model.insert_many(map(_item_to_row, _items_for_item))).execute()
            model.delete().where(self._primary_key.not_in(Latest.select(Latest.key))).execute()
        Latest.drop_table()
    
    def __len__(self):
        return self.count()
    
    def __iter__(self):
        return self.iter()
    
    def _opt(self, name, options={}):
        return options.get(name, self.options.get(name))
    
    def _item_to_row(self, item):
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
    
    def _row_to_item(self, row, options={}):
        if row is None:
            return row
        if self._opt('raw', options):
            return row
        ret = {
            field_name: getattr(row, field_name)
            for field_name in self._field_names
        }
        if self._has_auto_data_field:
            if data := getattr(row, self._auto_data_field):
                ret.update(json.loads(data))
        return ret
    
    def _get_row_key(self, row, getter=getattr):
        if self._has_auto_key_field:
            return getter(row, self._auto_key_field)
        else:
            return tuple(
                getter(row, field_name)
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
        renames = []
        if (old_name := self._opt('old_name')):
            if old_name in database.get_tables():
                renames.append((old_name.capitalize(), table_name.capitalize()))

        model = _model_from_options(self.options, table_name, database, renames=renames)
        
        if renames:
            migrate.sync(
                (model, renames),
                database=database,
                droptables=False,
            )

        if table_name in database.get_tables():
            if self._opt('_empty_schema'):
                model = self._database_models[table_name]  # just use existing table model
            else:
                old_model = self._database_models[table_name]

                def before_action(action):
                    match action.type:
                        case 'drop_column':
                            column_name = action.column_name
                            old_model.update(**{
                                self._auto_data_field: peewee.fn.json_set(
                                    peewee.fn.json(getattr(old_model, self._auto_data_field)),
                                    f'$.{column_name}',
                                    getattr(old_model, column_name),
                                ),
                            }).execute()
                
                def after_action(action):
                    match action.type:
                        case 'add_column':
                            field = getattr(model, self._auto_data_field)
                            column_name = action.column_name
                            model.update(**{
                                column_name: peewee.fn.json_extract(
                                    field,
                                    f"$.{column_name}",
                                ),
                                self._auto_data_field: peewee.fn.json_remove(
                                    field,
                                    f"$.{column_name}",
                                ),
                            }).execute()

                # minor todo: pass cached models to sync
                actions = migrate.sync(
                    model,
                    database=database,
                    droptables=False,
                    before_action=before_action,
                    after_action=after_action,
                )
        else:
            database.bind([model])
            database.create_tables([model])

        return model

    def _keep_rows_order_with_keys(self, rows, keys):
        key_to_index = {key: index for index, key in enumerate(keys)}
        return sorted(rows, key=lambda row: key_to_index[self._get_row_key(row)])
    
    def _get_data(self, key):
        query = self.model.select(
            getattr(self.model, self._auto_data_field),
        ).where(
            self.model._meta.primary_key == key
        )
        row = next(iter(query), None)
        if not row:
            return {}
        return self._row_to_item(row)
    
    @functools.cached_property
    def _database_models(self):
        return _cached(lambda: models_from_database(self.database), self._database_level_cache, 'models')
    
    @property
    def _primary_key(self):
        return self._meta.primary_key
    
    @property
    def _meta(self):
        return self.model._meta


def _model_from_options(options, table_name, database, *, renames=[]):
    body = {}
    
    for name, spec in options['fields'].items():
        body[name] = _model_field_from_field_spec(spec)
        if spec.get('old_name'):
            renames.append((spec['old_name'], name))
    
    meta_body = {}
    
    primary_key = options['primary_key']
    if isinstance(primary_key, (tuple, list)):
        meta_body['primary_key'] = peewee.CompositeKey(*primary_key)

    indexes = options['indexes']
    if indexes:
        meta_body['indexes'] = []
        for index in indexes:
            if isinstance(index, str):
                index = ((index,), False)
            elif isinstance(index, (tuple, list)):
                if isinstance(index[0], str):
                    index = (index, False)
            meta_body['indexes'].append(index)
    
    if meta_body:
        body['Meta'] = type('Meta', (), meta_body)

    return type(table_name, (peewee.Model,), body)


def _model_field_from_field_spec(spec):
    cls = _field_type_to_peewee_field_class(spec['type'])
    kwargs = {
        'primary_key': spec.get('primary_key', False),
        'index': spec.get('index', False),
        'null': spec.get('null', False),
    }
    return cls(**kwargs)


def _normalized_fields(options):
    fields = options.get('fields', {})

    for name, spec in fields.items():
        spec = _normalized_field_spec(name, spec)
        fields[name] = spec
        if spec.get('primary_key'):
            options['auto_key_field'] = None
            options['primary_key'] = name

    auto_key_field = options['auto_key_field']
    if auto_key_field and auto_key_field == options['primary_key']:
        fields[auto_key_field] = _normalized_field_spec(auto_key_field, options['auto_key_type'])
    
    auto_data_field = options['auto_data_field']
    if auto_data_field:
        fields[auto_data_field] = _normalized_field_spec(auto_data_field, 'str')
    
    primary_key = options['primary_key']
    composite_key = isinstance(primary_key, (tuple, list))
    for name, spec in fields.items():
        if not composite_key and name == primary_key:
            spec['primary_key'] = True

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


def _set_options_defaults(options, *, table_name=None, database=None):
    if table_name:
        options.setdefault('table', table_name)
    if database:
        options.setdefault('database', database)

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
    options.setdefault('auto_key_field', '_key')
    options.setdefault('auto_data_field', '_data')
    options.setdefault('primary_key', options['auto_key_field'])
    options.setdefault('database', ':memory:')
    options.setdefault('old_name', None)
    
    options['_empty_schema'] = 'fields' not in options

    options.setdefault('indexes', [])

    options['fields'] = _normalized_fields(options)
    
    return options
