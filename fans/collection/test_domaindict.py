import pytest

from fans.collection import DomainDict


def test_init():
    d = DomainDict({'foo': 3})
    assert d['foo'] == 3


def test_used_as_normal_dict():
    d = DomainDict()
    d['foo'] = 3
    assert d['foo'] == 3
    assert d.get('foo') == 3
    assert d.get('bar') is None
    del d['foo']
    assert d.get('foo') is None


def test_slice_set_get():
    d = DomainDict()
    d['id':42] = 3
    assert d[42] == 3
    assert d['id':42] == 3

    with pytest.raises(KeyError):
        d['foo':42]


def test_auto_domain_order():
    d = DomainDict()
    d['id':42] = 3
    d['name':42] = 5
    assert d[42] == 3


def test_manual_domain_order():
    d = DomainDict(domains = ['name', 'id'])
    d['id':42] = 3
    d['name':42] = 5
    assert d[42] == 5


def test_delete_all_value_for_key():
    d = DomainDict()
    d[42] = 1
    d['id':42] = 3
    d['name':42] = 5
    del d[42]
    assert d.get(42) is None


def test_reorder():
    d = DomainDict(domains = ['1', '2', '3', '4', '5'])
    d['1':'foo'] = 1
    d['2':'foo'] = 2
    d['3':'foo'] = 3
    assert d['foo'] == 1
    d.reorder('3', '2', ..., '1')
    assert d['foo'] == 3


def test_del_by_domain():
    d = DomainDict()
    d['1':'foo'] = 1
    d['2':'foo'] = 2
    assert d['foo'] == 1
    del d['1':'foo']
    assert d['foo'] == 2
