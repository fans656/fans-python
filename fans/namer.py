import itertools
from typing import Iterable


def ensure_names(
    items,
    getname=None,
    field='name',
    **kwargs,
):
    if not getname:
        getname = lambda d: d.get(field)

    has_missing = False
    exclude = set()
    for item in items:
        if field in item:
            name = item[field]
        else:
            name = item[field] = getname(item)
        if not name:
            has_missing = True
        exclude.add(item[field])
    
    if has_missing:
        _newnames = newnames(exclude=exclude)
        for item in items:
            if not item[field]:
                item[field] = next(_newnames)
    
    return items


def newnames(
    prefix='noname',
    sep='-',
    exclude=None,
    start=1,
) -> Iterable[str]:
    exclude = _make_exclude_pred(exclude)

    newname = prefix
    if not exclude(newname):
        yield newname

    for index in itertools.count(start):
        newname = f'{prefix}{sep}{index}'
        if not exclude(newname):
            yield newname


def _make_exclude_pred(exclude):
    if exclude is None:
        return lambda _: False

    if isinstance(exclude, Iterable):
        if not isinstance(exclude, set):
            exclude = set(exclude)
        return lambda d: d in exclude
    
    if callable(exclude):
        return exclude
    
    raise TypeError(f'invalid exclude {exclude}')
