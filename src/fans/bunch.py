class bunch(dict):
    """
    bunch is a dict with extra ability to:
        d['foo'] <=> d.foo
        d.foo = 3 <=> d['foo'] = 3
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__.update(self)

    def __setattr__(self, key, value):
        self[key] = value
        self.__dict__[key] = value
