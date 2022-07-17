noop = lambda *_, **__: None
identity = lambda x: x


def parse_int(value, default = 0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default
