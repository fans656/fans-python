import json

from fans.logger import get_logger

from .utils import merge_extend


logger = get_logger(__name__)


class Persist:

    def load(self, path, hint, **kwargs):
        with path.open() as f:
            return json.load(f, **kwargs)

    def save(self, path, data, hint, **kwargs):
        kwargs.setdefault('ensure_ascii', False)
        # TODO: atomic write
        with path.open('w') as f:
            json.dump(data, f, **kwargs)

    def extend(self, path, items, hint, key = None, **kwargs):
        if not items:
            return
        try:
            orig_items = self.load(path, hint)
            assert isinstance(orig_items, list)
        except:
            logger.exception('error loading original items while extend')
            orig_items = []
        if key:
            items = merge_extend(orig_items, items, key = key)
            if orig_items and orig_items[-1] == items[-1]:
                # no update if no change on last item
                # e.g. coingecko price update
                return
        self.save(path, items, hint, **kwargs)
        return True
