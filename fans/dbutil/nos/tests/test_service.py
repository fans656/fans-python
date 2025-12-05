from fans.dbutil.nos.service import (
    _normalized_conf,
)


def test_normalized_conf():
    # simple conf for single store
    assert _normalized_conf({
        'name': 'sample',
        'path': '~/sample.sqlite',
    }) == {
        'stores': [{
            'name': 'sample',
            'path': '~/sample.sqlite',
        }],
    }

    # stores in list form
    assert _normalized_conf([{
        'name': 'sample',
        'path': '~/sample.sqlite',
    }]) == {
        'stores': [{
            'name': 'sample',
            'path': '~/sample.sqlite',
        }],
    }

    # normalized form
    assert _normalized_conf({
        'stores': [{
            'name': 'sample',
            'path': '~/sample.sqlite',
        }],
    }) == {
        'stores': [{
            'name': 'sample',
            'path': '~/sample.sqlite',
        }],
    }
