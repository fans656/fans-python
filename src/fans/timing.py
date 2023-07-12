import time
import functools

from fans import fmt
from fans.logger import get_logger


default_logger = get_logger(__name__)


def timing(*args, **kwargs):
    """
    Log time of code execution.

    Sample usage:

        with timing():
            time.sleep(1)

        @timing
        def do_something():
            ...

        @timing('something is doing')
        def do_something():
            ...
    """
    if args and callable(args[0]):
        return _timing(*[args[0].__name__, *args[1:]], **kwargs)(args[0])
    else:
        return _timing(*args, **kwargs)


class _timing:

    def __init__(self, name = '', logger = default_logger):
        self.name = name
        self.name_out = f' {name}' if name else ''
        self.logger = logger

    def __enter__(self):
        self.beg = time.time()
        if self.name:
            self.logger.info(f"    beg{self.name_out}...")
        return self

    def __exit__(self, exc_cls, exc, trace):
        self.end = time.time()
        if exc:
            self.logger.info(f"... err{self.name_out} in {fmt.duration(self.elapsed)}")
        else:
            self.logger.info(f"... end{self.name_out} in {fmt.duration(self.elapsed)}")

    def __call__(self, func):
        """
        As decorator
        """
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapped

    @property
    def elapsed(self):
        self.end = time.time()
        return self.end - self.beg
