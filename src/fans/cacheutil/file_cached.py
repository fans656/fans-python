import pickle

from fans.path import Path
from fans.logger import get_logger


logger = get_logger(__name__)


def file_cached(calc, name = 'default', refresh = False):
    path = Path('/tmp/fans/cache') / name
    if refresh:
        path.remove()
    if path.exists():
        try:
            with path.open('rb') as f:
                return pickle.load(f)
        except:
            logger.warning(f'error loading pickle file {path}')
    ret = calc()
    path.ensure_parent()
    with path.open('wb') as f:
        pickle.dump(ret, f)
    return ret
