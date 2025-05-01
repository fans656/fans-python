import uuid
import base64
import traceback
from typing import Callable

import dill
from fans.logger import get_logger

from .context import Context


logger = get_logger(__name__)


def run_client(
        make_talks: str|Callable[[Context],None],
        request: Callable[[dict], dict],
):
    """
    Params:

        make_talks
            use str to specify a `<module>:<func>`
            or use callable directly

        request - function to send request and get response
    """
    ctx = Context()

    try:
        make_talks(ctx)
    except Exception as exc:
        logger.error(f'error when preparing talks: {exc}')
        return
    
    context_id = uuid.uuid4().hex
    req = {
        'context_id': context_id,
        'make_talks': base64.b64encode(dill.dumps(make_talks)).decode(),
        'req': ctx._talks[0](),
    }

    res = request(req)

    ctx._talks[2](res['res'])


def handle_request_on_server(req: dict):
    context_id = req['context_id']
    if context_id not in _context_id_to_ctx:
        make_talks = dill.loads(base64.b64decode(req['make_talks']))
        ctx = Context()
        make_talks(ctx)
        _context_id_to_ctx[context_id] = ctx

    try:
        res = ctx._talks[1](req['req'])
        return {
            'res': res,
        }
    except Exception as exc:
        return {
            'error': str(exc),
            'trace': traceback.format_exc(),
        }


_context_id_to_ctx = {}
