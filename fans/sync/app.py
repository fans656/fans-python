import importlib

from fastapi import FastAPI

from .sync import process_sync


app = FastAPI()


@app.post('/api/fans-sync')
def api_fans_sync(req: dict):
    op = req.get('op')
    
    match op:
        case 'sqlite':
            return handle_sqlite_sync(req)
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
            results['module'] = process_sync(actions_generator, Context.Remote())

    return {
        'results': results,
        'errors': errors,
    }
