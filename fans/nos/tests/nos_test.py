import os

from fans.nos import Nos


def test_reset_path(tmp_path):
    os.chdir(tmp_path)

    nos = Nos()
    assert str(nos.path) == 'nos.sqlite'
    assert str(nos.store.path) == 'nos.sqlite'

    nos.path = 'foo.sqlite'
    assert str(nos.path) == 'foo.sqlite'
    assert str(nos.store.path) == 'foo.sqlite'
