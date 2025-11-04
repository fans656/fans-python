import logging

import click

from fans.logger import get_logger
from fans.jober import Jober


logging.root.setLevel(logging.INFO)
logger = get_logger(__name__)


@click.group
def cli():
    pass


@cli.command()
@click.option('--host', default='127.0.0.1')
@click.option('-p', '--port', type=int, default=8000)
@click.option('--token')
@click.argument('config', required=False)
def serve(host: str, port: int, token: str, config: str):
    """Run in server mode"""
    import uvicorn

    from .app import root_app

    jober = Jober(config)
    Jober.set_instance(jober)
    
    if token:
        from fastapi import Request
        from fastapi.responses import JSONResponse

        @root_app.middleware('http')
        async def verify_token(request: Request, callnext):
            if request.cookies.get('token') != token:
                return JSONResponse(content={'error': 'invalid token'}, status_code=401)
            return await callnext(request)

        logger.info(f'using token {token}')

    uvicorn.run(root_app, host=host, port=port)


if __name__ == '__main__':
    cli()
