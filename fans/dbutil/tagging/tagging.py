"""
Tagging means attach some string tags to an entity in set, and later query sub-set entities using these tags.

For example given a set of numbers: [0 1 2 3 4 5 6 7 8 9]
- [0   2   4   6   8  ] can be tagged "even"
- [  1   3   5   7   9] can be tagged "odd"
- [    2 3   5   7    ] can be tagged "prime"

Then we can do query:
- "prime" -> [2 3 5 7]
- "odd" and "prime" -> [3 5 7]
- "even" or "prime" -> [0 2 3 4 5 6 7 8]

This utility use a sqlite table to store the tagging info and power the query.

To initialize, construct a `tagging` instance passing the (peewee) database and (optional) table name:

    tagging = dbutil.tagging(peewee.SqliteDatabase(':memory:'), 'person_tag')

By default entity is represented by `int` key:

    tagging.add_tag(1, 'odd')               # add single tag to single entity
    tagging.add_tag(2, 'even', 'prime')     # add multiple tags to single entity
    tagging.add_tag([3, 5, 7], 'prime')     # add single tag to multiple entities

you can also specify key type when constructing `tagging`:

    dbutil.tagging(database, key_type=str)
    dbutil.tagging(database, key_type=float)
    dbutil.tagging(database, key_type=(float, str))  # composite key

or by specifying target table:

    dbutil.tagging(database, target='person')
    dbutil.tagging(database, target=Person)  # using peewee model

Query entities is by the `.find` method:

    tagging.find('prime')  # => [2, 3, 5, 7]

The argument to `.find` is actually a boolean expression in string form:

    tagging.find('prime & odd')  # use '&' for AND
    tagging.find('prime odd')  # implicit AND

    tagging.find('even | prime')  # use '|' for OR

    tagging.find('odd & !prime')  # use '!' for NOT

    tagging.find('(even | prime) & odd')  # nested expression

To get all tags of a given entity, use `.tags(key)`:

    tagging.tags(2)  # ['even', 'prime']

without argument, `.tags()` return all existing tags:

    tagging.tags()  # ['even', 'odd', 'prime']
"""
import operator
import warnings
import itertools
import functools
from typing import Optional

import peewee
from fans.fn import chunks

from .parse import parse_query_expr


DEFAULT_TABLE_NAME = 'tag'


class tagging:

    def __init__(
        self,
        database: 'peewee.SqliteDatabase',
        table_name: str = DEFAULT_TABLE_NAME,
        *,
        target: str|peewee.Model = None,
        key=int,
        key_type=None,  # deprecated, use `key` instead
        tag_col: str = 'tag',
    ):
        assert isinstance(table_name, str)

        key_cols = []

        if key_type is not None:
            warnings.warn('`key_type` deprecated, use `key` instead', DeprecationWarning, stacklevel=2)
            key = None

        if key is not None:
            name_type_list_form = False
            if isinstance(key, (tuple, list)):
                if any(isinstance(d, (tuple, list)) for d in key):
                    assert all(isinstance(d, (tuple, list)) for d in key), f'specify all keys in (name, type) form'
                    name_type_list_form = True
            
            if name_type_list_form:
                key_cols, key_type = [], []
                for _key in key:
                    if isinstance(_key, (tuple, list)):
                        key_cols.append(_key[0])
                        key_type.append(_key[1])
                    else:
                        key_type.append(_key)
            elif isinstance(key, (tuple, list)):
                key_type = key
            else:
                key_type = key

        if target is not None:
            if isinstance(target, str):
                from fans.dbutil.introspect import models_from_database
                models = models_from_database(database)
                model = models[target]
            elif isinstance(target, peewee.ModelBase):
                model = target
            else:
                raise TypeError(f'unsupported target type {type(target)}')
            
            if table_name == DEFAULT_TABLE_NAME:
                table_name = f'{model._meta.table_name}_tag'

            primary_key = model._meta.primary_key

            if isinstance(primary_key, peewee.CompositeKey):
                key_type = [
                    _key_type_from_peewee_field(model._meta.fields[field_name])
                    for field_name in primary_key.field_names
                ]
                key_cols = primary_key.field_names
            else:
                key_type = _key_type_from_peewee_field(primary_key)
                key_cols = [primary_key.column_name]
        
        is_composite_key = isinstance(key_type, (tuple, list)) and len(key_type) > 1
        
        if not key_cols:
            if is_composite_key:
                key_cols = [f"key{i}" for i in range(len(key_type))]
            else:
                key_cols = ['key']

        self.database = database
        self.table_name = table_name
        self.is_composite_key = is_composite_key
        self.key_cols = key_cols
        self.key_types = key_type if is_composite_key else [key_type]
        self.tag_col = _tag_col_from_key_cols(key_cols, tag_col)
        self.cols = [*self.key_cols, self.tag_col]
        self.model = self._make_model(database, table_name)

        self.database.bind([self.model])
        self.database.create_tables([self.model])

    def add_tag(self, arg, *tags, chunk_size=500):
        """
        Add tag(s) for given key(s).

        Single key, single tag:
        
            tagging.add_tag(1, 'foo')
            tagging.add_tag((1, 1.0), 'foo')  # use tuple for composite key
        
        Single key, multiple tags:
        
            tagging.add_tag(1, 'foo', 'bar')
        
        Multiple keys, single tag:
        
            tagging.add_tag([1, 2], 'foo')  # use list for multiple keys
        
        Multiple keys, multiple tags:
        
            tagging.add_tag([1, 2], 'foo', 'bar')
        
        Batch mode (more performant):
        
            tagging.add_tag([(1, 'foo'), (2, 'bar')])  # tag as last value in item tuple
            tagging.add_tag([(1, 1.0, 'foo'), (2, 2.0, 'bar')])  # composite key (flatten)
            tagging.add_tag([((1, 1.0), 'foo'), ((2, 2.0), 'bar')])  # composite key (non-flatten)
        """
        if tags:
            keys = arg if isinstance(arg, list) else [arg]
            items = _ensure_flat_tuples(itertools.product(keys, tags))
            self.model.insert_many(items).on_conflict_ignore().execute()
        else:  # batch mode
            items = arg
            for chunk in chunks(_ensure_flat_tuples(items), chunk_size):
                self.model.insert_many(chunk).on_conflict_ignore().execute()
    
    def remove_tag(self, key, *tags, chunk_size=50):
        """
        Remove tag(s) for given key(s).
        
        Single key, single tag:
        
            tagging.remove_tag(1, 'foo')
        
        Single key, multiple tags:
        
            tagging.remove_tag(1, 'foo', 'bar')
        
        Single key, all tags:
        
            tagging.remove_tag(1)
        
        Multiple keys, single tag:
        
            tagging.remove_tag([1, 2], 'foo')
        
        Multiple keys, multiple tags:
        
            tagging.remove_tag([1, 2], 'foo', 'bar')
        
        Multiple keys, all tags:
        
            tagging.remove_tag([1, 2])
        """
        keys = key if isinstance(key, list) else [key]

        if tags:
            keys = _ensure_flat_tuples(itertools.product(keys, tags))
            cols = self.cols
        else:
            cols = self.key_cols

        for chunk in chunks(keys, chunk_size):
            cols_str = ','.join(cols)
            vals_str = ','.join([_as_sql_tuple(d) for d in chunk])
            pred_str = ' and '.join([
                f'{self.table_name}.{col} = to_delete.{col}' for col in cols
            ])
            sql = f'''
                with to_delete({cols_str}) as (values {vals_str})
                delete from {self.table_name}
                where exists (
                    select 1 from to_delete where {pred_str}
                )
            '''
            self.database.execute_sql(sql)

    def find(self, expr: str, return_query: bool = False):
        m = self.model
        key_fields = [getattr(m, key_col) for key_col in self.key_cols]
        query = m.select(*key_fields)

        res = parse_query_expr(expr)

        if res['has_or'] or res['has_and'] or res['has_not']:
            tree = res['tree']
            if res['has_or'] and not (res['has_and'] or res['has_not']):  # simple OR expr
                query = query.where(m.tag << tree['subs'])
            else:  # complex expr
                query = query.group_by(*key_fields).having(_tree_to_having_cond(tree, m))
        else:  # single tag query
            query = query.where(m.tag == expr)

        if return_query:
            return query
        else:
            if self.is_composite_key:
                return [tuple(getattr(d, key_col) for key_col in self.key_cols) for d in query]
            else:
                return [getattr(d, self.key_cols[0]) for d in query]

    def tags(self, key=None) -> list[str]:
        """
        Get all existing tags, or tags of a given key.
        """
        m = self.model
        query = m.select(m.tag).distinct()
        if key is not None:
            if self.is_composite_key:
                query = query.where(
                    functools.reduce(operator.and_, [
                        getattr(m, key_col) == key[i]
                        for i, key_col in enumerate(self.key_cols)
                    ])
                )
            else:
                query = query.where(getattr(m, self.key_cols[0]) == key)
        return [d.tag for d in query]

    def _make_model(self, database, table_name):
        Meta = type('Meta', (), {
            'primary_key': peewee.CompositeKey(*self.key_cols, self.tag_col),
        })

        body = {'Meta': Meta}
        for key_col, key_type in zip(self.key_cols, self.key_types):
            body[key_col] = _key_type_to_peewee_field(key_type)
        body[self.tag_col] = peewee.TextField(index=True)

        return type(table_name, (peewee.Model,), body)


def _tree_to_having_cond(tree, m):
    if isinstance(tree, str):
        return peewee.fn.sum(m.tag == tree) == 1
    elif isinstance(tree, dict):
        conds = [_tree_to_having_cond(sub, m) for sub in tree['subs']]
        match tree['type']:
            case 'and':
                return functools.reduce(operator.and_, conds)
            case 'or':
                return functools.reduce(operator.or_, conds)
            case 'not':
                return ~conds[0]
            case _:
                raise ValueError(f"Unknown operator type: {op_type}")
    else:
        raise TypeError(f"Invalid tree node type: {type(tree)}")


def _key_type_to_peewee_field(key_type):
    if key_type is int:
        return peewee.IntegerField()
    elif key_type is str:
        return peewee.TextField()
    elif key_type is float:
        return peewee.FloatField()
    else:
        raise ValueError(f'unsupported key type {key_type}')


def _key_type_from_peewee_field(field):
    match field.field_type:
        case 'INT'|'AUTO':
            return int
        case 'TEXT':
            return str
        case 'FLOAT':
            return float
        case _:
            raise NotImplementedError(f'unsupported field type {field}')


def _tag_col_from_key_cols(key_cols: list[str], tag_col: str):
    ret = tag_col
    key_cols = set(key_cols)
    for i in itertools.count():
        if ret not in key_cols:
            break
        ret = f'{tag_col}{i}'
    return ret


def _ensure_flat_tuples(items):
    items = iter(items)
    item = next(items, None)
    if item is None:
        return
    if isinstance(item, (tuple, list)) and len(item) == 2 and isinstance(item[0], (tuple, list)):
        yield from ((*d[0], d[1]) for d in (item, *items))
    else:
        yield from (item, *items)


def _as_sql_tuple(value):
    return f"({str(value).lstrip('(').rstrip(')')})"
