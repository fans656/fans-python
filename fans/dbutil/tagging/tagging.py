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

    tagging.add_tag(1, 'odd')
    tagging.add_tag(2, 'even', 'prime')
    tagging.add_tag([3, 5, 7], 'prime')

but you can also specify key type when constructing `tagging`:

    dbutil.tagging(peewee.SqliteDatabase(':memory:'), key_type=str)

    dbutil.tagging(peewee.SqliteDatabase(':memory:'), key_type=float)

    dbutil.tagging(peewee.SqliteDatabase(':memory:'), key_type=(float, str))  # composite key

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
import itertools
import functools
from typing import Optional

import peewee
from fans.fn import chunks

from .parse import parse_query_expr


_EntityKey = str | int | float
EntityKey = _EntityKey | tuple[_EntityKey]


class tagging:

    def __init__(
        self,
        database: 'peewee.SqliteDatabase',
        table_name: str = 'tag',
        key_type: type | list[type] = int,
    ):
        self.database = database
        self.table_name = table_name
        self.key_type = key_type
        self.is_composite_key = isinstance(key_type, (tuple, list))
        self.key_cols = [
            f"key{i}" for i in range(len(key_type))
        ] if self.is_composite_key else ['key0']
        self.model = self._make_model(database, table_name, key_type)

        self.database.bind([self.model])
        self.database.create_tables([self.model])

    def add_tag(self, keys_or_key, *tags, chunk_size=500):
        if tags:
            if isinstance(keys_or_key, list):
                keys = keys_or_key
            else:
                keys = [keys_or_key]

            items = list(itertools.product(keys, tags))
            if self.is_composite_key:
                items = [_item_from_tuple(*d) for d in items]

            self.model.insert_many(items).on_conflict_ignore().execute()
        else:
            tag_items = keys_or_key
            for chunk in chunks(tag_items, chunk_size):
                self.model.insert_many(chunk).on_conflict_ignore().execute()

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
                return [d.key0 for d in query]

    def tags(self, key: Optional[EntityKey] = ...):
        m = self.model
        query = m.select(m.tag).distinct()
        if key is not ...:
            if self.is_composite_key:
                query = query.where(
                    functools.reduce(operator.and_, [
                        getattr(m, key_col) == key[i]
                        for i, key_col in enumerate(self.key_cols)
                    ])
                )
            else:
                query = query.where(m.key0 == key)
        return [d.tag for d in query]

    def _make_model(self, database, table_name, key_type):
        Meta = type('Meta', (), {
            'primary_key': peewee.CompositeKey(*self.key_cols, 'tag'),
        })

        cls_body = {
            'Meta': Meta,
        }
        if self.is_composite_key:
            for i, _key_type in enumerate(key_type):
                cls_body[f"key{i}"] = _key_type_to_peewee_field(_key_type)
        else:
            cls_body['key0'] = _key_type_to_peewee_field(key_type)

        cls_body['tag'] = peewee.TextField(index=True)

        Model = type(table_name, (peewee.Model,), cls_body)

        return Model


def _key_type_to_peewee_field(key_type):
    if key_type is int:
        return peewee.IntegerField()
    elif key_type is str:
        return peewee.TextField()
    elif key_type is float:
        return peewee.FloatField()
    else:
        raise ValueError(f'unsupported key type {key_type}')


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


def _item_from_tuple(sub_keys, tag):
    ret = {'tag': tag}
    for i, sub_key in enumerate(sub_keys):
        ret[f"key{i}"] = sub_key
    return ret
