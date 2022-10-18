import pickle

from fans.logger import get_logger


logger = get_logger(__name__)


class Persist:

    def load(self, path, hint, **kwargs):
        with path.open('rb') as f:
            return pickle.load(f, **kwargs)

    def save(self, path, data, hint, **kwargs):
        with path.open('wb') as f:
            pickle.dump(data, f, **kwargs)
