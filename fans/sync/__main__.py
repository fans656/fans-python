import click


@click.command
@click.option('--serve', is_flag=True, help='Run sync server')
@click.option('--host', default='127.0.0.1', help='Server listening host')
@click.option('--port', type=int, default=8000, help='Server listening port')
@click.argument('modules', nargs=-1)
def cli(modules, serve, host, port):
    """
    Sync data from remote to local
    """
    if serve:
        import uvicorn

        from .app import app

        uvicorn.run(app, host=host, port=port)
    else:
        if not modules:
            print('ERROR: empty modules specified')
            exit(1)
        pass


if __name__ == '__main__':
    cli()
