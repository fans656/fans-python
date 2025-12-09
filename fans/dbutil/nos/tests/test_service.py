from fans.dbutil.nos.service import (
    Service,
    _normalized_conf,
)
from fans.dbutil.nos import Nos


class Test_Service:
    
    def test_get_nos(self):
        service = Service.get_instance(fresh=True)
        service.setup({
            'name': 'sample',
            'path': ':memory:',
        })

        service = Service.get_instance()
        assert isinstance(service.get_store('sample'), Nos)
        assert not service.get_store('foo')


def test_normalized_conf():
    # simple conf for single store
    assert _normalized_conf({
        'name': 'sample',
        'path': '~/sample.sqlite',
    }) == {
        'default_store_enabled': True,
        'stores': [{
            'name': 'sample',
            'path': '~/sample.sqlite',
        }, {
            'name': 'default',
            'path': ':memory:',
        }],
    }

    # stores in list form
    assert _normalized_conf([{
        'name': 'sample',
        'path': '~/sample.sqlite',
    }]) == {
        'default_store_enabled': True,
        'stores': [{
            'name': 'sample',
            'path': '~/sample.sqlite',
        }, {
            'name': 'default',
            'path': ':memory:',
        }],
    }

    # normalized form
    assert _normalized_conf({
        'stores': [{
            'name': 'sample',
            'path': '~/sample.sqlite',
        }],
    }) == {
        'default_store_enabled': True,
        'stores': [{
            'name': 'sample',
            'path': '~/sample.sqlite',
        }, {
            'name': 'default',
            'path': ':memory:',
        }],
    }
