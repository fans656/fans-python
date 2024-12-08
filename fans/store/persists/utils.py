import os
import itertools
import tempfile
import contextlib
from bisect import bisect_left
from typing import Callable, Any


class keyed:

    def __init__(self, items, _key: str = None, *, key: Callable[[Any], Any] = None):
        self.items = items
        if _key:
            self.get_key = lambda item: item[_key]
        elif key:
            self.get_key = key
        else:
            raise ValueError('not key provided')

    def __getitem__(self, i):
        return self.get_key(self.items[i])

    def __len__(self):
        return len(self.items)


class atomic_write:

    def __init__(self, path, mode = None):
        self.path = path
        self.tmp_path = (path.parent / (path.name + '.tmp'))
        self.mode = mode

    def __enter__(self):
        if self.mode:
            self.tmp_f = self.tmp_path.open(self.mode)
            return self.tmp_f
        else:
            return self.tmp_path

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            if self.tmp_path.exists():
                self.tmp_path.unlink()
        else:
            self.tmp_path.rename(self.path)


def merge_extend(xs, ys, key = lambda x: x):
    if not ys:
        return None
    ys = dedup_keeping_first(ys, key)
    xs = xs[:bisect_left(keyed(xs, key = key), key(ys[0]))]
    return xs + ys


dedup_keeping_first = lambda xs, key: list(reversed({key(x): x for x in reversed(xs)}.values()))
