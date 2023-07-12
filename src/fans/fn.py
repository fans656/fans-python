import json
import hashlib
from typing import Iterable, List


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


def omit(d: dict, keys: Iterable[str]):
    return {key: value for key, value in d.items() if key not in keys}


def chunks(vs: Iterable[any], chunk_size: int, count: bool = False) -> Iterable[List[any]]:
    chunk = []
    for i, v in enumerate(vs):
        chunk.append(v)
        if len(chunk) == chunk_size:
            if count:
                yield (i + 1 - chunk_size, i + 1), chunk
            else:
                yield chunk
            chunk = []
    if chunk:
        if count:
            yield (i + 1 - len(chunk), i + 1), chunk
        else:
            yield chunk
