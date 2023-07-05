from typing import Union, Callable

from fans.fn import noop


class Meta(dict):
    """
    Usage:

        from fans.path import Path
        meta = Path('meta.json').as_meta(default = lambda: {'foo': 3})
        meta['bar'] = 5
        meta.save({'baz': 8})
    """

    def __init__(
            self,
            path: 'fans.Path',
            default: Callable[[], dict] = lambda: {},
            before_save: Callable[[dict], None] = noop,
            tmpdir: 'fans.Path' = None,
    ):
        self.path = path
        self.default = default
        self.loaded = False
        self.before_save = before_save
        self.tmpdir = tmpdir

    def save(self, update: dict = None):
        if not self.loaded:
            self.load()
        meta = {**self, **(update or {})}
        self.before_save(meta)
        self.update(meta)
        self._save()

    def load(self):
        try:
            self.update(self.path.load(hint = 'json'))
        except:
            self.update(self.default())
            self._save()
        self.loaded = True
        return self

    def get(self, *args, **kwargs):
        if not self.loaded:
            self.load()
        return super().get(*args, **kwargs)

    def getitems(self):
        if not self.loaded:
            self.load()
        return super().getitems()

    def values(self):
        if not self.loaded:
            self.load()
        return super().values()

    def _save(self):
        self.path.save(
            self,
            hint = {'persist': 'json', 'tmpdir': self.tmpdir},
            indent = 2,
            ensure_ascii = False,
        )

    def __getitem__(self, *args, **kwargs):
        if not self.loaded:
            self.load()
        return super().__getitem__(*args, **kwargs)
