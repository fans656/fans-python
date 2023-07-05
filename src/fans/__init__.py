from .fn import noop


def ellipsis(string: str, length: int = 80, suffix = '...'):
    """
    Truncate string to given length.

    >>> ellipsis('hello world', 5)
    'he...'
    >>> ellipsis('hello world', 5, '')
    'hello'
    """
    if len(string) > length:
        return string[:length - len(suffix)] + suffix
    else:
        return string


if __name__ == '__main__':
    import doctest
    doctest.testmod()
