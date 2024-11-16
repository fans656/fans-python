from typing import Iterable, List


def vectorized(obj, **kwargs):
    if isinstance(obj, Iterable):
        return Vectorized(obj, **kwargs)
    elif callable(obj):
        return lambda *args, **_kwargs: Vectorized(obj(*args, **_kwargs), **kwargs)
    else:
        raise ValueError(f"invalid vectorize target {obj}")


class Vectorized:

    def __init__(self, xs):
        self.__xs = xs

    def __bool__(self):
        return bool(self.__xs)

    def __len__(self):
        return len(self.__xs)

    def __iter__(self) -> any:
        return iter(self.__xs)

    def __getattr__(self, key) -> any:
        return self.__class__(getattr(x, key, None) for x in self.__xs)

    def __call__(self, *args, **kwargs) -> List[any]:
        return self.__class__(list(x(*args, **kwargs) for x in self.__xs))

    def __repr__(self):
        return f'Vectorized({self.__xs})'
