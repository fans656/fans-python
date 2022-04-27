import time
import threading
from typing import Iterable

from fans.logger import get_logger
from fans import fmt


default_logger = get_logger(__name__)
default_interval = 5


def progress(*args, **kwargs):
    if isinstance(args[0], int):
        return _progress(*args, **kwargs)
    elif isinstance(args[0], list) or hasattr(args[0], '__len__') and isinstance(args[0], Iterable):
        return iter_on_list(*args, **kwargs)
    elif len(args) >= 2 and isinstance(args[0], Iterable) and isinstance(args[1], int):
        return iter_on_list(args[0], *args[2:], n = args[1], **kwargs)
    else:
        raise ValueError(f'invalid arguments: {args} {kwargs}')


class _progress:

    env = threading.local()

    def __init__(self, n, logger = None, interval = None, verbose = False):
        self.n = n
        self.logger = logger
        self.interval = interval
        self.verbose = verbose

        self.i = 0
        self._factor = 1

    def iter(self, iterable):
        for item in iterable:
            self.i += 1
            yield item

    def info(self, message):
        self._show(message)

    def warning(self, *args, **kwargs):
        self.logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        self.logger.error(*args, **kwargs)

    def critical(self, *args, **kwargs):
        self.logger.critical(*args, **kwargs)

    def exception(self, *args, **kwargs):
        self.logger.exception(*args, **kwargs)

    def step(self, message = ''):
        self.i += 1
        self._show(message)

    def __call__(self, *args, **kwargs):
        return progress(*args, **kwargs)

    def _show(self, message):
        if self.verbose:
            should_show = True
        elif self.interval <= 0:
            should_show = True
        elif len(self.env.stack) == 1 and self.i == self.n:
            should_show = True
        else:
            now = time.time()
            elapsed = now - self.env.last_show_time
            should_show = elapsed >= self.interval
            if should_show:
                self.env.last_show_time = now
        if should_show:
            pct = sum(pg.i / pg.n * pg._factor for pg in self.env.stack)
            elapsed = time.time() - self.env.begin_time
            remain = elapsed / pct * (1 - pct) if pct else -1
            self.logger.info(
                f"{pct * 100:6.2f}% {fmt.duration(elapsed)} + {fmt.duration(remain)} | {message}"
            )

    def __enter__(self):
        if not hasattr(self.env, 'stack'):
            self.env.stack = []
            self.env.last_show_time = 0
            self.env.begin_time = time.time()
        stack = self.env.stack
        if stack:
            pg = stack[-1]
            if self.logger is None:
                self.logger = pg.logger
            if self.interval is None:
                self.interval = pg.interval
            self._factor = pg._factor / pg.n

        if self.logger is None:
            self.logger = default_logger
        if self.interval is None:
            self.interval = default_interval

        self.env.stack.append(self)
        return self

    def __exit__(self, *_, **__):
        self.env.stack.pop()


def iter_on_list(xs, n = None, *args, **kwargs):
    with _progress(n or len(xs), *args, **kwargs) as pro:
        for x in pro.iter(xs):
            yield x, pro
