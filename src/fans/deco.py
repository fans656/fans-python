def singleton(cls):
    instances = {}
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return get_instance



def ensure_not_none(message):
    def deco(func):
        def wrapped(*args, **kwargs):
            ret = func(*args, **kwargs)
            if ret is None:
                if callable(message):
                    msg = message(*args, **kwargs)
                else:
                    msg = message
                raise ValueError(msg)
            return ret
        return wrapped
    return deco


def override(func):
    return func
