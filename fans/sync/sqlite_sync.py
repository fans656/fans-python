import io
import uuid
import base64
import sqlite3
import subprocess
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
    req = {
        'op': 'sqlite',
        'database': database,
        'table': table,
        'ts_columns': ts_columns,
        'when': when,
    }
    if local_database_path and not Path(local_database_path).exists():
        req['require_schema'] = True

    url = f'{origin}/api/fans-sync'

    res = requests.post(url, json=req).json()
    
    # TODO: wget data file
    items = list(load_items(res))
    
    if local_database_path:
        if not Path(local_database_path).exists():
            _create_local_database(local_database_path, res['schema'])
        _save_to_local_database(local_database_path, table=table, items=items, columns=res['columns'])
    else:
        for item in items:
            print(item)


def handle_sqlite_sync_server_side(req: dict, root=None, paths=None):
    """
    Params:
    
        req - {
            database: str - [REQUIRED] database path or name in `paths` specified in `sync.setup_server(paths=...)`
            table: str - [REQUIRED] table name
            ts_columns: list[str] - timestamp columns used to determine rows to sync, defaults to ['added']
            when: int - timestamp to filter rows (newer than this timestamp) to sync, defaults to 0
            fields: list[str] - columns to return, defaults to all columns
            require_schema: bool - whether require table schema in response, used to build table on client side
        }
    
    Returns:
        
        dict {
            type: str - 'file'
            data: str - rows data file path 
        } or {
            type: str - 'inline'
            data: list[list] - rows data
        }
    """
    database = req['database']
    if paths and hasattr(paths, database):
        database = getattr(paths, database)
    elif root:
        database = Path(root) / database

    table_name = req['table']
    kwargs = {
        'database': str(database),
        'table': table_name,
    }
    for key in ['ts_columns', 'when', 'fields']:
        if key in req:
            kwargs[key] = req[key]

    count, cursor = get_items_later_than(**kwargs)
    
    ret = dump_items(cursor, **req.get('dump', {}))
    
    if req.get('require_schema'):
        ret['schema'] = subprocess.check_output(f'sqlite3 {database} ".schema"', shell=True).decode()
    
    columns = [
        d[1] for d in sqlite3.connect(database).execute(
            f'PRAGMA table_info({table_name})'
        ).fetchall()
    ]
    ret['columns'] = columns
    
    return ret


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


def _create_local_database(database_path, schema):
    assert subprocess.run(
        f'sqlite3 {database_path}',
        input=schema.encode(),
        shell=True,
    ).returncode == 0


def _save_to_local_database(database_path, *, table, items, columns):
    columns_str = ','.join(columns)
    placeholders = ','.join(['?' for _ in columns])

    sql = f'insert into {table} ({columns_str}) values ({placeholders})'
    conn = sqlite3.connect(database_path)
    conn.executemany(sql, list(items))
    conn.commit()


def _get_database(database: str|peewee.SqliteDatabase):
    if isinstance(database, (str, Path)):
        return peewee.SqliteDatabase(database)
    else:
        return database
