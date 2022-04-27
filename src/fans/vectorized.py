from typing import Iterable
from collections import deque

from fans.fn import noop


def vectorized(obj, **kwargs):
    if isinstance(obj, Iterable):
        return Vectorized(obj, **kwargs)
    elif callable(obj):
        return lambda *args, **_kwargs: Vectorized(obj(*args, **_kwargs), **kwargs)
    else:
        raise ValueError(f"invalid vectorize target {obj}")


class Vectorized:

    def __init__(self, iterable, collect = True):
        self.__iterable = iterable
        self.__collecting = collect

    def __bool__(self):
        return bool(self.__iterable)

    def __iter__(self):
        return iter(self.__iterable)

    def __len__(self):
        return len(self.__iterable)

    def __getattr__(self, key):
        try:
            return getattr(self.__iterable, key)
        except AttributeError:
            def func(*args, **kwargs):
                for item in self.__iterable:
                    yield getattr(item, key)(*args, **kwargs)
            if self.__collecting:
                return lambda *args, **kwargs: list(func(*args, **kwargs))
            else:
                return lambda *args, **kwargs: deque(func(*args, **kwargs), maxlen = 0) or None
