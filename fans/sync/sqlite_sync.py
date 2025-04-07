import io
import uuid
import base64
from pathlib import Path

import peewee
import msgpack
import requests


DEFAULT_TS_COLUMNS = ['added']


def handle_sqlite_sync_client_side(
        origin: str,
        database: str,
        table: str,
        *,
        ts_columns: list[str] = DEFAULT_TS_COLUMNS,
        when: int = 0,
        local_database_path: str = None,
        **__,
):
    url = f'{origin}/api/fans-sync'
    res = requests.post(url, json={
        'op': 'sqlite',
        'database': database,
        'table': table,
        'ts_columns': ts_columns,
        'when': when,
    })
    
    items = load_items(res.json())
    
    if local_database_path:
        #if not Path(local_database_path).exists():
        #    _create_local_database(local_database_path, )
        _save_to_local_database(local_database_path, items)
    else:
        for item in items:
            print(item)


def handle_sqlite_sync_server_side(req: dict, paths=None):
    database = req['database']
    if paths and hasattr(paths, database):
        database = getattr(paths, database)

    kwargs = {
        'database': str(database),
        'table': req['table'],
    }
    for key in ['ts_columns', 'when', 'fields']:
        if key in req:
            kwargs[key] = req[key]

    count, cursor = get_items_later_than(**kwargs)
    
    return dump_items(cursor, **req.get('dump', {}))


def get_items_later_than(
        database: str|peewee.SqliteDatabase,
        table: str,
        ts_columns: list[str] = DEFAULT_TS_COLUMNS,
        when: int = 0,
        fields: list[str] = (),
):
    database = _get_database(database)
    
    if fields:
        fields_sql = ','.join(fields)
    else:
        fields_sql = '*'
    
    ts_columns_sql = ' or '.join(f'{d} > {when}' for d in ts_columns)
    where_sql = f' where {ts_columns_sql}'

    count = database.execute_sql(f'select count(*) from {table} {where_sql}').fetchone()[0]
    cursor = database.execute_sql(f'select {fields_sql} from {table} {where_sql}')
    
    return count, cursor


def dump_items(
        cursor,
        threshold: int = 32 * 1024 * 1024,  # 32 MB
        json_compatible: bool = True,
):
    fpath = None
    f = None

    buf = io.BytesIO()
    for row in cursor:
        buf.write(msgpack.packb(row))
        n_bytes = buf.getbuffer().nbytes
        if n_bytes > threshold:
            fpath = f'/tmp/{uuid.uuid4().hex}'
            f = open(fpath, 'wb')
            f.write(buf.getvalue())
            break
    
    if fpath:
        for row in cursor:
            f.write(msgpack.packb(row))
        f.close()
        return {'type': 'file', 'data': fpath}
    else:
        data = buf.getvalue()
        if json_compatible:
            data = base64.b64encode(data)
        return {'type': 'inline', 'data': data}


def load_items(dumpped: dict):
    match dumpped['type']:
        case 'inline':
            yield from msgpack.Unpacker(io.BytesIO(base64.b64decode(dumpped['data'])))
        case 'file':
            with open(dumpped['data'], 'rb') as f:
                yield from msgpack.Unpacker(f)


def _save_to_local_database(database_path, items):
    database = _get_database(database_path)


def _get_database(database: str|peewee.SqliteDatabase):
    if isinstance(database, str):
        return peewee.SqliteDatabase(database)
    else:
        return database
