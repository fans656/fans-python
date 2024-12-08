class bunch(dict):
    """
    A dict where value can be accessed by attribute.

    >>> b = bunch({'foo': 3})
    >>> b.foo
    3
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__.update(self)

    def __setattr__(self, key, value):
        self[key] = value
        self.__dict__[key] = value

    def __getattr__(self, key):
        return self.get(key)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
