from fastapi import APIRouter

from .sync import sync


app = APIRouter()


@app.post('/api/fans-sync')
async def api_fans_sync(req: dict):
    return sync.server.handle_sync_request(req)
