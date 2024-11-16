import json
import hashlib

from fans.logger import get_logger

from .utils import merge_extend


logger = get_logger(__name__)


class Persist:

    def load(self, path, hint = None, **kwargs):
        with path.open(encoding = 'utf-8') as f:
            return json.load(f, **kwargs)

    def save(self, path: 'pathlib.Path', data: any, hint: dict, **kwargs):
        """
        Save data to path as json

        Write will be atomic if `hint` having:
            tmpdir: pathlib.Path - directory used for atomic writing intermediate results
        """
        kwargs.setdefault('ensure_ascii', False)

        tmpdir = (hint or {}).get('tmpdir')
        if tmpdir:
            write_path = tmpdir / hashlib.md5(str(path).encode()).hexdigest()
        else:
            write_path = path

        with write_path.open('w', encoding = 'utf-8') as f:
            json.dump(data, f, **kwargs)

        if write_path != path:
            write_path.replace(path)

    def extend(self, path, items, hint, key = None, **kwargs):
        if not items:
            return
        if path.exists():
            try:
                orig_items = self.load(path, hint)
                assert isinstance(orig_items, list)
            except:
                logger.exception('error loading original items while extend')
                orig_items = []
        else:
            orig_items = []
        if key:
            items = merge_extend(orig_items, items, key = key)
            if orig_items and orig_items[-1] == items[-1]:
                # no update if no change on last item
                # e.g. coingecko price update
                return
        self.save(path, items, hint, **kwargs)
        return True
