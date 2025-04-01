import io
import uuid
import base64

import peewee
import msgpack


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


def get_items_later_than(
        database: str|peewee.SqliteDatabase,
        table: str,
        column: str,
        when: int = 0,
        fields: list[str] = (),
):
    database = _get_database(database)
    
    if fields:
        fields_sql = ','.join(fields)
    else:
        fields_sql = '*'

    count = database.execute_sql(f'''
        select count(*) from {table} where {column} > {when}
                         ''').fetchone()[0]

    cursor = database.execute_sql(f'''
        select {fields_sql} from {table} where {column} > {when}
                         ''')
    
    return count, cursor


def _get_database(database: str|peewee.SqliteDatabase):
    if isinstance(database, str):
        return peewee.SqliteDatabase(database)
    else:
        return database
