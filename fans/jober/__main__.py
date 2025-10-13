import logging

import click
from fastapi import FastAPI

from .jober import Jober


logging.root.setLevel(logging.INFO)


@click.group
def cli():
    pass


@cli.command()
@click.option('--host', default='127.0.0.1')
@click.option('-p', '--port', type=int, default=8000)
@click.argument('config', required=False)
def serve(host: str, port: int, config: str):
    """Run in server mode"""
    import uvicorn

    from .app import root_app

    jober = Jober(config)
    Jober.set_instance(jober)

    uvicorn.run(root_app, host=host, port=port)


if __name__ == '__main__':
    cli()
