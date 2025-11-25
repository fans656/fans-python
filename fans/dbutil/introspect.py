"""
See also .venv/bin/pwiz.py
"""
import peewee
from playhouse.reflection import Introspector

from fans.bunch import bunch


def models_from_database(database: 'peewee.SqliteDatabase'):
    ret = bunch()
    introspector = Introspector.from_database(database)
    meta = introspector.introspect()
    for table_name, model_name in meta.model_names.items():
        ret[table_name] = _create_model_class(table_name, model_name, meta, introspector)
    tables = list(ret.values())
    database.bind(tables)
    return ret


def _create_model_class(table_name, model_name, meta, introspector):
    body = {}

    primary_keys = meta.primary_keys[table_name]
    
    if len(primary_keys) > 1:
        body['Meta'] = type('Meta', (), {
            'primary_key': peewee.CompositeKey(*primary_keys),
        })

    for name, column in meta.columns[table_name].items():
        if (
            name in primary_keys
            and name == 'id'
            and len(primary_keys) == 1
            and column.field_class in introspector.pk_classes
        ):
            continue

        if column.primary_key and len(primary_keys) > 1:
            column.primary_key = False

        body[name] = column.field_class(**column.get_field_parameters())

    return type(model_name, (peewee.Model,), body)
