import peewee
import playhouse.sqlite_ext
from playhouse import migrate

from fans.logger import get_logger


logger = get_logger(__name__)


def sync(*models):
    """
    Sync model with database.

    Currently supported operations:
        - migrate from empty table
        - add column
        - drop column
    """
    for model in models:
        Syncer(model).sync()


class Syncer:

    def __init__(self, model):
        self.model = model

        self.meta = self.model._meta
        self.table_name = self.meta.name
        self.database = self.meta.database
        self.migrator = migrate.SqliteMigrator(self.database)

    def sync(self):
        if is_empty_table(self.model):
            self.database.drop_tables([self.model])
            self.database.create_tables([self.model])
            return

        self.operations = []

        src_fields = get_fields(self.model)
        dst_fields = self.meta.sorted_fields

        fields_to_add = list(get_fields_to_add(src_fields, dst_fields))
        fields_to_del = list(get_fields_to_del(src_fields, dst_fields))
        get_fields_to_change(src_fields, dst_fields, fields_to_add, fields_to_del)

        self.drop(*fields_to_del)
        self.add(*fields_to_add)

        for op in self.operations:
            logger.info(f"{op.method} {op.args} {op.kwargs}")
        migrate.migrate(*self.operations)

    def add(self, *fields):
        self.operations.extend(
            self.migrator.add_column(
                self.table_name,
                field.name,
                field,
            ) for field in fields
        )

    def drop(self, *fields):
        self.operations.extend(
            self.migrator.drop_column(
                self.table_name,
                field.name,
                field,
            ) for field in fields
        )


def get_fields_to_add(src_fields, dst_fields):
    src_field_names = set(d.name for d in src_fields)
    for dst_field in dst_fields:
        if dst_field.name == 'id':
            continue
        if dst_field.name not in src_field_names:
            yield dst_field


def get_fields_to_del(src_fields, dst_fields):
    dst_fields_names = set(d.name for d in dst_fields)
    for src_field in src_fields:
        if src_field.name == 'id':
            continue
        if src_field.name not in dst_fields_names:
            yield src_field


def get_fields_to_change(src_fields, dst_fields, to_add, to_del):
    name_to_dst_field = {d.name: d for d in dst_fields}
    for src_field in src_fields:
        if src_field.name == 'id':
            continue
        dst_field = name_to_dst_field.get(src_field.name)
        if not dst_field:
            continue
        dst_data_type = dst_field_to_data_type[type(dst_field)]
        if src_field.data_type != dst_data_type:
            to_del.append(src_field)
            to_add.append(dst_field)


def is_primary_key(*fields):
    return any(field.primary_key for field in fields)


def is_empty_table(model):
    fields = get_fields(model)
    primary_key_field = next((field for field in fields if is_primary_key(field)), None)
    if not primary_key_field:
        return True
    return model.select(primary_key_field).count() == 0


def get_fields(model):
    return model._meta.database.get_columns(model._meta.name)


dst_field_to_data_type = {
    peewee.AutoField: 'AUTO',
    peewee.TextField: 'TEXT',
    peewee.FloatField: 'REAL',
    peewee.IntegerField: 'INTEGER',
    peewee.BooleanField: 'BOOLEAN',
    peewee.BinaryUUIDField: 'BLOB',
    playhouse.sqlite_ext.JSONField: 'JSON',
}
