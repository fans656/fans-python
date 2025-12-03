import uuid
import functools
import contextlib
from typing import List, Tuple

import peewee
from playhouse import migrate
from fans.fn import noop
from fans.bunch import bunch
from fans.dbutil.introspect import models_from_database


__all__ = [
    'sync',
]


def sync(
    *models,
    database=None,
    droptables=True,
    before_action=noop,
    after_action=noop,
    dryrun: bool = False,
) -> list[dict]:
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
    
    Returns:
        A list of performed actions.
    """
    performed_actions = []

    if not models:
        return performed_actions

    if database is None:
        for model in models:
            if isinstance(model, (tuple, list)):
                model = model[0]
            if model._meta.database:
                database = model._meta.database
                break
    
    if database is None:
        raise ValueError('no database given')

    table_names = set()
    
    execute_action = functools.partial(
        _execute_action,
        database=database,
        actions=performed_actions,
        before_action=before_action,
        after_action=after_action,
    )

    existed_models = models_from_database(database)
    for model in models:
        model = _sync_model(
            model,
            database,
            execute_action=execute_action,
            existed_models=existed_models,
        )
        table_names.add(model.table_name)

    # drop extra tables
    if droptables:
        extra_names = set(database.get_tables()) - table_names
        if extra_names:
            with database.atomic():
                for name in extra_names:
                    execute_action(bunch({
                        'type': 'drop_table',
                        'table_name': name,
                    }))

    return performed_actions


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

    @contextlib.contextmanager
    def using_table_name(self, new_table_name):
        old_table_name = self.table_name
        self.table_name = new_table_name
        yield
        self.table_name = old_table_name

    @property
    def src_col_names(self):
        return [col.name for col in self.database.get_columns(self.table_name)]

    @property
    def src_col_names_sql(self):
        names = [d for d in self.src_col_names if d != 'id']
        return ','.join(names)

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


def _sync_model(model: peewee.Model, database, *, execute_action, existed_models):
    if isinstance(model, tuple):
        model, renames = model
    else:
        model, renames = model, []
    
    if not model._meta.database:
        database.bind([model])

    model = Model(model, renames)
    migrator = migrate.SqliteMigrator(database)
    
    execute_action = functools.partial(execute_action, migrator=migrator)

    with database.atomic():
        # create table
        if not model.table_rename and not database.table_exists(model.table_name):
            execute_action({
                'type': 'create_table',
                'model': model.model,
            })

        # rename table
        if model.table_rename:
            execute_action({
                'type': 'rename_table',
                'model': model,
            })

        # rename columns
        for src_name, dst_name in model.column_renames:
            execute_action({
                'type': 'rename_column',
                'model': model,
                'src_name': src_name,
                'dst_name': dst_name,
            })

        # change primary key
        src_primary_keys = database.get_primary_keys(model.table_name)
        dst_primary_keys = [field.name for field in model.meta.get_primary_keys()]
        if src_primary_keys != dst_primary_keys:
            execute_action({
                'type': 'change_primary_key',
                'model': model,
            })

        src_col_names = set(model.src_col_names)
        dst_col_names = set(model.dst_col_names)

        # add columns
        add_names = dst_col_names - src_col_names
        name_to_dst_col = {col.name: col for col in model.dst_cols}
        for name in add_names:
            execute_action({
                'type': 'add_column',
                'table_name': model.table_name,
                'column_name': name,
                'column': name_to_dst_col[name],
            })

        src_indexes = set(model.src_indexes)
        dst_indexes = set(model.dst_indexes)

        # add indexes
        add_indexes = dst_indexes - src_indexes
        for index in add_indexes:
            execute_action({
                'type': 'add_index',
                'table_name': model.table_name,
                'index': index,
            })

        # del indexes
        del_indexes = src_indexes - dst_indexes
        cols_to_index = {
            tuple(index.columns): index for index in database.get_indexes(model.table_name)
        }
        for cols in del_indexes:
            index = cols_to_index[cols]
            if index.unique:
                continue
            execute_action({
                'type': 'drop_index',
                'table_name': model.table_name,
                'index_name': index.name,
            })

        # del columns
        del_names = src_col_names - dst_col_names
        for name in del_names:
            execute_action({
                'type': 'drop_column',
                'table_name': model.table_name,
                'column_name': name,
            })
        
        old_model = existed_models.get(model.table_name)
        if old_model:
            # change column types
            for field_name, field in model.meta.fields.items():
                old_field = old_model._meta.fields.get(field_name)
                if not old_field:
                    continue
                if not _need_change_column(old_field, field):
                    execute_action({
                        'type': 'change_column',
                        'table_name': model.table_name,
                        'column_name': field_name,
                        'old_field': old_field,
                        'new_field': field,
                    })

            exp_indexes = {d._name: d for d in model.meta.fields_to_index()}
            got_indexes = {d._name: d for d in old_model._meta.fields_to_index()}
            exp_index_names = set(exp_indexes.keys())
            got_index_names = set(got_indexes.keys())

            ## add indexes
            #for index_name in exp_index_names - got_index_names:
            #    execute_action({
            #        'type': 'add_index',
            #        'table_name': model.table_name,
            #        'index': index_name,
            #    })

            ## drop indexes
            #for index_name in got_index_names - exp_index_names:
            #    execute_action({
            #        'type': 'drop_index',
            #        'table_name': model.table_name,
            #        'index_name': index_name,
            #    })

    return model


def _need_change_column(x, y):
    for attr_name in ['field_type', 'null']:
        if getattr(x, attr_name) != getattr(y, attr_name):
            return False
    return True


def _execute_action(
    action,
    *,
    database,
    dryrun: bool = False,
    actions: list[dict] = [],
    before_action=noop,
    after_action=noop,
    migrator=None,
):
    action = bunch(action)
    if not dryrun:
        before_action(action)
        match action.type:
            case 'create_table':
                database.create_tables([action.model])
            case 'drop_table':
                database.execute_sql(f'drop table {action.table_name}')
            case 'rename_table':
                migrate.migrate(migrator.rename_table(
                    *map(peewee.make_snake_case, action.model.table_rename)
                ))
            case 'add_column':
                migrate.migrate(migrator.add_column(action.table_name, action.column_name, action.column))
            case 'drop_column':
                migrate.migrate(migrator.drop_column(action.table_name, action.column_name))
            case 'rename_column':
                migrate.migrate(migrator.rename_column(
                    action.model.table_name, action.src_name, action.dst_name,
                ))
            case 'add_index':
                migrate.migrate(migrator.add_index(action.table_name, action.index))
            case 'drop_index':
                migrate.migrate(migrator.drop_index(action.table_name, action.index_name))
            case 'change_primary_key':
                model = action.model
                if model.model.select().count() == 0:
                    database.execute_sql(f'drop table {model.table_name}')
                    database.create_tables([model.model])
                else:
                    tmp_name = f'tmp_{uuid.uuid4().hex}'
                    table_name = model.table_name

                    database.execute_sql(f'alter table {table_name} rename to {tmp_name}')
                    database.create_tables([model.model])

                    with model.using_table_name(tmp_name):
                        sql = f'''
                            insert into {table_name} ({model.src_col_names_sql})
                            select {model.src_col_names_sql} from {tmp_name}
                        '''
                        database.execute_sql(sql)

                    database.execute_sql(f'drop table {tmp_name}')
            case 'change_column':
                new_field = action.new_field
                field = new_field.__class__(
                    null=new_field.null,
                    index=new_field.index,
                    unique=new_field.unique,
                    default=new_field.default,
                    primary_key=new_field.primary_key,
                    constraints=new_field.constraints,
                    sequence=new_field.sequence,
                    collation=new_field.collation,
                    unindexed=new_field.unindexed,
                    index_type=new_field.index_type,
                )
                migrate.migrate(
                    migrator.alter_column_type(action.table_name, action.column_name, field),
                )
            case _:
                raise ValueError(f'unknown action {action}')
        after_action(action)
    actions.append(action)
