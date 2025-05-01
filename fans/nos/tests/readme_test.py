import pytest

from fans.nos import nos


@pytest.fixture(autouse=True)
def initialize_test(tmp_path):
    database_path = tmp_path / 'nos.sqlite'
    nos.path = database_path
    print(database_path)


def test_put_get_delete():
    # initially can not found
    assert nos.get('foo') is None

    # put something
    nos.put({'name': 'foo', 'age': 3})

    # can get
    assert nos.get('foo') == {'name': 'foo', 'age': 3}

    # can delete
    nos.delete('foo')

    # deleted
    assert nos.get('foo') is None


def test_domains():
    foos = nos.domain('foo')
    foos.put({'name': 'foo1'})
    assert len(foos) == 1

    bars = nos.domain('bar')
    bars.put({'name': 'bar1'})
    assert len(bars) == 1


def test_label():
    nos.put({'name': 'foo'})
    nos.put({'name': 'bar'})
    nos.put({'name': 'baz'})

    nos.label('foo', {'city': 'chengdu'})
    nos.label('baz', {'city': 'chengdu'})

    assert nos.search({'label': {'city': 'chengdu'}}) == [
        {'name': 'baz'},
        {'name': 'foo'},
    ]


def test_tag():
    nos.put({'name': 'foo'})
    nos.put({'name': 'bar'})
    nos.put({'name': 'baz'})

    nos.tag('bar', 'm5')

    assert nos.search({'tag': 'm5'}) == [
        {'name': 'bar'},
    ]


def test_list():
    nos.put({'name': 'foo'})
    nos.put({'name': 'bar'})

    nos.put({'name': '1'}, domain='number')
    nos.put({'name': '2'}, domain='number')

    assert list(nos.list()) == [
        {'name': 'bar'},
        {'name': 'foo'},
    ]

    assert list(nos.list('number')) == [
        {'name': '1'},
        {'name': '2'},
    ]


def test_list_domains():
    nos.put({'name': 'foo'})
    nos.put({'name': '1'}, domain='number')
    assert set(nos.domains) == set(['default', 'number'])


def test_field_link():
    """
    doc can have nested sub-docs, e.g.

        name: Nirvana
        albums:
          - name: Nevermind  # sub-doc
          - name: Bleach     # sub-doc
    """
    nos.put({'name': 'Nirvana'}, domain='artist')

    nos.put({'name': 'Nevermind'}, domain='album')
    nos.put({'name': 'Bleach'}, domain='album')

    nos.link(('artist', 'Nirvana'), ('album', 'Nevermind'), 'albums', field=True)
    nos.link(('artist', 'Nirvana'), ('album', 'Bleach'), 'albums', field=True)

    doc = nos.get('Nirvana', domain='artist', fields=['albums'])
    assert doc['name'] == 'Nirvana'
    assert doc['albums'] == [
        {'name': 'Nevermind'},
        {'name': 'Bleach'},
    ]
