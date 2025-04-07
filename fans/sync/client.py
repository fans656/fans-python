from . import sqlite_sync


class Client:

    def __init__(self):
        pass
    
    def sync(self, target: dict):
        match target.get('type'):
            case 'sqlite':
                return sqlite_sync.handle_sqlite_sync_client_side(**target)
            case _:
                raise ValueError(f'unsupported target type "{target["type"]}"')
