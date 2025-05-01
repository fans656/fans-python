import json

import click

from fans.sync import sync


@click.command
@click.option('--serve', is_flag=True, help='Run sync server')
@click.option('--host', default='127.0.0.1', help='Server listening host')
@click.option('--port', type=int, default=8000, help='Server listening port')
@click.option('--root', default='.', help='Server root directory path')
@click.argument('args', nargs=-1)
def cli(args, serve, host, port, root):
    """
    Sync data from remote to local
    """
    if serve:
        import uvicorn

        from .app import app
        
        sync.setup_server(root=root)

        uvicorn.run(app, host=host, port=port)
    else:
        if not args:
            print('ERROR: empty request specified')
            exit(1)
        
        req = args[0]
        if req.startswith('{') and req.endswith('}'):
            req = json.loads(req)
        sync(req)


if __name__ == '__main__':
    cli()
