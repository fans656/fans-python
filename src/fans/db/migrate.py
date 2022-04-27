from typing import List, Tuple

import peewee
from playhouse import migrate
from fans.logger import get_logger


logger = get_logger(__name__)


class Model:

    def __init__(self, model: peewee.Model, renames = None):
        self.model = model
        self.meta = model._meta
        self.table_name = self.meta.table_name
        self.database = self.meta.database

        self.table_rename = None
        self.column_renames = []
        for src_name, dst_name in renames or ():
            if src_name[0].isupper():
                self.table_rename = (src_name, dst_name)
            else:
                self.column_renames.append((src_name, dst_name))

    @property
    def src_col_names(self):
        return [col.name for col in self.database.get_columns(self.table_name)]

    @property
    def dst_col_names(self):
        return self.meta.sorted_field_names

    @property
    def dst_cols(self):
        return self.meta.sorted_fields

    @property
    def src_indexes(self):
        for index in self.database.get_indexes(self.table_name):
            yield tuple(index.columns)

    @property
    def dst_indexes(self):
        for col in self.dst_cols:
            if col.index:
                yield (col.name,)
        for index in self.meta.indexes:
            yield index[0]


def sync_model(model: peewee.Model):
    if isinstance(model, tuple):
        model, renames = model
    else:
        renames = []

    model = Model(model, renames)
    database = model.database
    migrator = migrate.SqliteMigrator(database)

    with database.atomic():
        # create table
        if not model.table_rename and not database.table_exists(model.table_name):
            database.create_tables([model.model])

        # rename table
        if model.table_rename:
            logger.info('rename table')
            migrate.migrate(migrator.rename_table(*map(peewee.make_snake_case, model.table_rename)))

        # rename columns
        for src_name, dst_name in model.column_renames:
            logger.info(f'rename column {src_name} -> {dst_name}')
            migrate.migrate(migrator.rename_column(model.table_name, src_name, dst_name))

        # change primary key
        src_primary_keys = database.get_primary_keys(model.table_name)
        dst_primary_keys = [field.name for field in model.meta.get_primary_keys()]
        if src_primary_keys != dst_primary_keys:
            if model.model.select().count() == 0:
                database.execute_sql(f'drop table {model.table_name}')
                database.create_tables([model.model])
            else:
                raise RuntimeError(f"cannot change primary keys on non-empty table")

        src_col_names = set(model.src_col_names)
        dst_col_names = set(model.dst_col_names)

        # add columns
        add_names = dst_col_names - src_col_names
        name_to_dst_col = {col.name: col for col in model.dst_cols}
        for name in add_names:
            col = name_to_dst_col[name]
            logger.info(f'add column {name}')
            migrate.migrate(migrator.add_column(model.table_name, name, col))

        # del columns
        del_names = src_col_names - dst_col_names
        for name in del_names:
            logger.info(f'del column {name}')
            migrate.migrate(migrator.drop_column(model.table_name, name))

        src_indexes = set(model.src_indexes)
        dst_indexes = set(model.dst_indexes)

        # add indexes
        add_indexes = dst_indexes - src_indexes
        for index in add_indexes:
            logger.info(f'add index {index}')
            migrate.migrate(migrator.add_index(model.table_name, index))

        # del indexes
        del_indexes = src_indexes - dst_indexes
        cols_to_index = {
            tuple(index.columns): index for index in database.get_indexes(model.table_name)
        }
        for cols in del_indexes:
            index = cols_to_index[cols]
            if index.unique:
                continue
            logger.info(f'del index {index.name}')
            migrate.migrate(migrator.drop_index(model.table_name, index.name))

    return model


def sync(*models, droptables = True):
    """
    Each model is one of following types:
        peewee.Model
        (peewee.Model, renames: List[Tuple[str, str]])

    renames is a list of (src_name, dst_name) tuple,
    capitalized names means table rename, non-capitalized names means column rename.

    Sample:
        Rename table Foo to Bar:
            sync((Bar, [('Foo', 'Bar')]))
        Rename column one to two:
            sync((Foo, [('one', 'two')]))
    """
    database = None
    names = []
    for model in models:
        model = sync_model(model)
        names.append(model.table_name)
        if model.database:
            database = model.database

    # drop extra tables
    if droptables and database:
        extra_names = set(database.get_tables()) - set(names)
        with database.atomic():
            for name in extra_names:
                database.execute_sql(f'drop table {name}')
