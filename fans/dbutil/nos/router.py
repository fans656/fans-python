from fastapi import APIRouter, HTTPException

from .service import Service


app = APIRouter()


@app.get('/api/nos/info')
def info():
    return Service.get_instance().info()


@app.post('/api/nos/put')
def put(store: str = 'default', collection: str = 'default', data: dict = None):
    _nos(store).put(data, collection=collection)


@app.get('/api/nos/count')
def count(store: str = 'default', collection: str = 'default'):
    return _nos(store).count(collection=collection)


def _nos(store):
    nos = Service.get_instance().get(store)
    if not nos:
        raise HTTPException(404, f'store "{store}" not found')
    return nos
