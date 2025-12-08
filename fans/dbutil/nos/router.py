import json

from fastapi import APIRouter, HTTPException
from fans.bunch import bunch

from .service import Service


Key = str|int|float


app = APIRouter()


@app.get('/api/nos/info')
def info_():
    return Service.get_instance().info()


@app.post('/api/nos/put')
def put_(data: dict|list[dict], options: str = '{}', store: str = 'default', collection: str = 'default'):
    return _nos(store).put(data, collection=collection, **json.loads(options))


@app.get('/api/nos/get')
def get_(key: Key, store: str = 'default', collection: str = 'default'):
    return _nos(store).get(key, collection=collection)


@app.post('/api/nos/update')
def update_(key: str, store: str = 'default', collection: str = 'default', update: dict = None):
    return _nos(store).update(key, update, collection=collection)


@app.post('/api/nos/remove')
def remove_(key: str, store: str = 'default', collection: str = 'default'):
    return _nos(store).remove(key, collection=collection)


@app.get('/api/nos/count')
def count_(store: str = 'default', collection: str = 'default'):
    return _nos(store).count(collection=collection)


@app.get('/api/nos/list')
def list_(store: str = 'default', collection: str = 'default'):
    return _nos(store).list(collection=collection)


@app.post('/api/nos/tag')
def tag_(key: str, store: str = 'default', collection: str = 'default'):
    return _nos(store).remove(key, collection=collection)


@app.post('/api/nos/create_store')
def create_store_(spec: dict):
    Service.get_instance().create_store(bunch(spec))


def _nos(name: str):
    nos = Service.get_instance().get_store(name)
    if not nos:
        raise HTTPException(404, f'store "{name}" not found')
    return nos
