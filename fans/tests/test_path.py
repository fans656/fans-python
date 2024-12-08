import pathlib

from fans.path import enhanced
from fans.path.enhanced import Path


def test_enhanced():
    # out of str
    path = enhanced('foo')
    assert isinstance(path, Path)
    assert str(path) == 'foo'

    # out of pathlib.Path
    path = enhanced(pathlib.Path('foo'))
    assert isinstance(path, Path)
    assert str(path) == 'foo'

    # out of enhancedpath
    path = Path(Path('foo'))
    assert isinstance(path, Path)
    assert str(path) == 'foo'
