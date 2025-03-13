import click

from .jober import Jober


@click.group
def cli():
    pass


@cli.command()
@click.option('-p', '--port', type=int, default=8000)
@click.option('-c', '--config')
def serve(port: int, config: str):
    """Start jober as HTTP server"""
    import uvicorn

    from .app import app

    Jober.get_instance(conf=config)

    uvicorn.run(app, port=port)


if __name__ == '__main__':
    cli()
