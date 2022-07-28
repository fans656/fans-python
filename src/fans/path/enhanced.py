import sys
import types
import pathlib
from typing import Callable


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

    def watch(self, on_event: Callable[['watchdog.events.FileSystemEvent'], None], now = True):
        import threading
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler

        class Handler(FileSystemEventHandler):

            def on_any_event(self, event):
                on_event(event)

        def do_watch():
            observer.start()
            observer.join()

        observer = Observer()
        observer.schedule(Handler(), self)
        thread = threading.Thread(target = do_watch, daemon = True)
        if now:
            thread.start()
        return thread

    def __getattr__(self, key):
        return getattr(self.store, key)


class ThisModule(types.ModuleType):

    def __call__(self, *args, **kwargs):
        return Path(*args, **kwargs)


sys.modules[__name__].__class__ = ThisModule
