#import functools
#
#import peewee

from fans.db.migrate import sync
#from fans.dbutil.introspect import models_from_database
#
#
#def add_table(database, *models):
#    database.bind(models)
#    database.create_tables(models)
#
#
#def drop_table(database, *names: list[str]):
#    names = set(names)
#    for table_name in database.get_tables():
#        if table_name in names:
#            database.execute_sql(f'drop table {table_name}')
#
#
#class Migration:
#    
#    def __init__(self, database, specs=[], **options):
#        self.database = database
#        self.targets = [Target(spec, self) for spec in specs]
#
#        options.setdefault('drop_table', True)
#
#        self.options = options
#    
#    def execute(self, dryrun: bool = False):
#        database = self.database
#        
#        self._dryrun = dryrun
#        self._performed_actions = []
#
#        for target in self.targets:
#            if target.got_model is None:
#                self._execute_action('add_table', {'model': target.exp_model})
#
#        if self.options['drop_table']:
#            exp_table_names = set(d.exp_table_name for d in self.targets)
#            got_table_names = set(database.get_tables())
#            extra_table_names = got_table_names - exp_table_names
#            if extra_table_names:
#                self._execute_action('drop_table', {'names': extra_table_names})
#
#        return self._performed_actions
#    
#    @functools.cached_property
#    def models(self):
#        return models_from_database(self.database)
#    
#    def _execute_action(self, action_type, data):
#        if not self._dryrun:
#            match action_type:
#                case 'add_table':
#                    add_table(self.database, data['model'])
#                case 'drop_table':
#                    drop_table(self.database, *data['names'])
#        self._performed_actions.append({'type': action_type, 'data': data})
#
#
#class Target:
#    
#    def __init__(self, arg, migration):
#        exp_model = got_model = conf = None
#        if isinstance(arg, peewee.ModelBase):
#            exp_model = arg
#        elif isinstance(arg, tuple):
#            if len(arg) == 3:
#                exp_model, got_model, conf = arg
#            elif len(arg) == 2:
#                if isinstance(arg[1], peewee.ModelBase):
#                    exp_model, got_model = arg
#                else:
#                    exp_model, conf = arg
#            else:
#                raise TypeError(f'invalid target {arg}')
#        else:
#            raise TypeError(f'invalid target {arg}')
#
#        self.exp_model = exp_model
#        self.exp_table_name = exp_model._meta.table_name
#        self.got_model = got_model or migration.models.get(self.exp_table_name)
#        self.migration = migration
