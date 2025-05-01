import sys
import importlib
from pathlib import Path
from typing import Callable, Iterator

from .sqlite_sync import handle_sqlite_sync_server_side
from .context import Context
from .action import Action
    

class Server:
    
    def __init__(
            self,
            root: str = '.',
            paths: 'fans.path.paths.NamespacedPath' = None,
    ):
        self.root = Path(root)
        self.paths = paths

    def handle_sync_request(self, req: dict):
        match req.get('op'):
            case 'sqlite':
                return handle_sqlite_sync_server_side(
                    req,
                    root=self.root,
                    paths=self.paths,
                )
            case _:
                return _run_sync(req)


def _run_sync(req):
    results = {}
    errors = []
    modules = req['syncs']
    for module in modules:
        try:
            mod = importlib.import_module(module)
        except Exception:
            errors.append({
                'err': f'import error {module}'
            })
        else:
            actions_generator = getattr(mod, 'sync', None)
            if not actions_generator:
                errors.append({
                    'err': f'no `sync` callable in {module}',
                })
                continue
            results['module'] = _process_sync(actions_generator, Context.Remote())

    return {
        'results': results,
        'errors': errors,
    }


def _process_sync(sync: Callable[[Context], Iterator[Action]], ctx: Context):
    actions = [d for d in sync(ctx) if d.side == ctx.side]
    for i_action, action in enumerate(actions):
        action.execute()

        if 'error' in action.result:
            print(action.result['trace'], file=sys.stderr)
            logger.error(action.result['error'])
            break

        if action.result.get('data') is not None or i_action != len(actions) - 1:
            ctx.communicate(action)
