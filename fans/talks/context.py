import contextlib


class Context:

    def __init__(self):
        self._current_talk = None
        self._talks = []
    
    def client(self, func):
        return self._add_talk(func, side='client')
    
    def server(self, func):
        return self._add_talk(func, side='server')
    
    @property
    def loop(self):
        @contextlib.contextmanager
        def _loop():
            print('beg collect loop talks')
            yield
            print('end collect loop talks')
        return _loop()
    
    @property
    def _last_talk(self):
        if not self._talks:
            self._talks.append(Seq())
        return self._talks[-1]
    
    def _add_talk(self, func, *, side: str):
        if side == self._last_talk.side:
            self._talks.append(Seq())
        self._last_talk.add_func(func, side)
        return func


class Talk:

    def __init__(self):
        self._funcs = []
        self.side = None
    
    def add_func(self, func, side):
        self._funcs.append(func)
        self.side = side


class Seq(Talk):

    pass
