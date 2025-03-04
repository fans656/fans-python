from typing import Union, Callable

from fans.fn import noop


class Meta(dict):
    """
    Usage:

        from fans.path import Path
        meta = Path('meta.json').as_meta()  # >>> {'foo': 3}
        meta['bar'] = 5  # >>> {'foo': 3, 'bar': 5}
        meta.save()
    """

    def __init__(
            self,
            path: 'fans.Path',
            default: Callable[[], dict] = lambda: {},
            before_save: Callable[[dict], None] = noop,
            save_kwargs: dict = {},
    ):
        self.path = path
        self.before_save = before_save
        self.save_kwargs = save_kwargs

        try:
            self.update(self.path.load())
        except:
            self.update(_to_value(default))

    def save(self, **kwargs):
        self.before_save(self)
        self.path.save(self, **{
            **self.save_kwargs,
            **kwargs,
        })


def _to_value(src):
    if callable(src):
        return src()
    else:
        return src
