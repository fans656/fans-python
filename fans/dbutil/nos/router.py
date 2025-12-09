import json

from fastapi import APIRouter, HTTPException, Depends
from fans.bunch import bunch

from .service import Service


app = APIRouter()


@Depends
def collection_dep(store: str = 'default', collection: str = 'default'):
    nos = Service.get_instance().get_store(store)
    if not nos:
        raise HTTPException(404, f'store "{name}" not found')
    return nos.collection(collection)


@Depends
def key_dep(key: str|int|float, parse: bool = True, c=collection_dep):
    if parse and isinstance(key, str) and key.startswith('[') and key.endswith(']'):
        key = json.loads(key)
        if key:
            if isinstance(key[0], list):
                key = [tuple(d) for d in key]
            elif c.is_composite_key:
                key = tuple(key)
    return key


@Depends
def options_dep(options: str = None):
    if options is None:
        return {}
    return json.loads(options)


@app.get('/api/nos/info')
def info_():
    return Service.get_instance().info()


@app.post('/api/nos/put')
def put_(data: dict|list[dict], options=options_dep, c=collection_dep):
    return c.put(data, **options)


@app.get('/api/nos/get')
def get_(key=key_dep, c=collection_dep):
    return c.get(key)


@app.post('/api/nos/update')
def update_(key=key_dep, update: dict = ..., c=collection_dep):
    return c.update(key, update)


@app.post('/api/nos/remove')
def remove_(key=key_dep, c=collection_dep):
    return c.remove(key)


@app.get('/api/nos/count')
def count_(c=collection_dep):
    return c.count()


@app.get('/api/nos/list')
def list_(c=collection_dep):
    return c.list()


@app.post('/api/nos/create_store')
def create_store_(spec: dict):
    Service.get_instance().create_store(bunch(spec))
