from fastapi import APIRouter, HTTPException

from .service import Service


app = APIRouter()


@app.get('/api/nos/info')
def info_():
    return Service.get_instance().info()


@app.post('/api/nos/put')
def put_(store: str = 'default', collection: str = 'default', data: dict = None):
    _nos(store).put(data, collection=collection)


@app.get('/api/nos/get')
def get_(key: str, store: str = 'default', collection: str = 'default'):
    return _nos(store).get(key, collection=collection)


@app.post('/api/nos/update')
def update_(key: str, store: str = 'default', collection: str = 'default', update: dict = None):
    _nos(store).update(key, update, collection=collection)


@app.post('/api/nos/remove')
def remove_(key: str, store: str = 'default', collection: str = 'default'):
    _nos(store).remove(key, collection=collection)


@app.get('/api/nos/count')
def count_(store: str = 'default', collection: str = 'default'):
    return _nos(store).count(collection=collection)


@app.get('/api/nos/list')
def list_(store: str = 'default', collection: str = 'default'):
    return _nos(store).list(collection=collection)


def _nos(store):
    nos = Service.get_instance().get(store)
    if not nos:
        raise HTTPException(404, f'store "{store}" not found')
    return nos
