from fastapi import APIRouter

from .talks import handle_request_on_server


router = APIRouter()


@router.post('/api/fans/talks')
def api_talks(req: dict):
    return handle_request_on_server(req)
