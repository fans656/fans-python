import sys
import types
import pathlib


class Path(type(pathlib.Path())):

    def ensure_parent(self):
        self.parent.mkdir(parents = True, exist_ok = True)

    def ensure_dir(self):
        self.mkdir(parents = True, exist_ok = True)

    def remove(self):
        if self.exists():
            self.unlink()

    @property
    def mtime(self):
        try:
            return self.stat().st_mtime
        except FileNotFoundError:
            return 0

    @property
    def store(self):
        from fans.store import Store
        return Store(self)

    def __getattr__(self, key):
        return getattr(self.store, key)


class ThisModule(types.ModuleType):

    def __call__(self, *args, **kwargs):
        return Path(*args, **kwargs)


sys.modules[__name__].__class__ = ThisModule
