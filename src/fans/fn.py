import json
import hashlib


noop = lambda *_, **__: None
identity = lambda x: x
pred_true = lambda _: True
pred_false = lambda _: False


def parse_int(value, default = 0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def calc_dict_md5(data):
    text = json.dumps(data, sort_keys = True, ensure_ascii = False)
    return hashlib.md5(text.encode()).hexdigest()


def partition(xs, pred = identity):
    ts, fs = [], []
    for x in xs:
        if pred(x):
            ts.append(x)
        else:
            fs.append(x)
    return ts, fs
