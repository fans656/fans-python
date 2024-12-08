import sys
import types
import pathlib
from typing import Callable

from .meta import Meta


class Path(type(pathlib.Path())):

    def ensure_parent(self):
        self.parent.mkdir(parents = True, exist_ok = True)
        return self

    def ensure_dir(self):
        self.mkdir(parents = True, exist_ok = True)
        return self

    def ensure_file(self):
        if not self.exists():
            self.ensure_parent()
            self.touch()
        return self

    def remove(self):
        if self.exists():
            self.unlink()

    def as_meta(self, **kwargs):
        return Meta(self, **kwargs)

    def with_tree(self, *args, **kwargs) -> 'fans.path.paths.NamespacedPath':
        from .paths import make_paths
        return make_paths(self, *args, **kwargs)

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

    def watch(
            self,
            on_event: Callable[['watchdog.events.FileSystemEvent'], None],
            now = True,
    ):
        import threading
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler

        class Handler(FileSystemEventHandler):

            def on_any_event(self, event):
                on_event(event)

        observer = Observer()
        observer.schedule(Handler(), self)
        if now:
            observer.start()
        return observer

    def on_modified(self, callback: Callable[[], any], now = True):
        def on_event(event):
            if not event.is_directory and event.event_type == 'modified':
                callback()
        self.watch(on_event)

    def __getattr__(self, key):
        return getattr(self.store, key)


class ThisModule(types.ModuleType):

    def __call__(self, *args, **kwargs):
        return Path(*args, **kwargs)


sys.modules[__name__].__class__ = ThisModule
