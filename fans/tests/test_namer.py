from fans import namer


def test_newnames():
    newnames = namer.newnames()
    assert next(newnames) == 'noname'
    assert next(newnames) == 'noname-1'
    assert next(newnames) == 'noname-2'

    newnames = namer.newnames(exclude=['noname'])
    assert next(newnames) == 'noname-1'
    assert next(newnames) == 'noname-2'
    assert next(newnames) == 'noname-3'

    newnames = namer.newnames(exclude=['noname-1'])
    assert next(newnames) == 'noname'
    assert next(newnames) == 'noname-2'
    assert next(newnames) == 'noname-3'


def test_ensure_names():
    assert namer.ensure_names([
        {'name': 'foo'},
        {},
        {'name': 'baz'},
        {},
    ]) == [
        {'name': 'foo'},
        {'name': 'noname'},
        {'name': 'baz'},
        {'name': 'noname-1'},
    ]
