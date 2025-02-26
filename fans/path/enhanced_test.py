import pathlib

from fans.path import Path


class Test_Path:

    def test_init(self):
        # out of str
        path = Path('foo')
        assert isinstance(path, Path)
        assert str(path) == 'foo'

        # out of pathlib.Path
        path = Path(pathlib.Path('foo'))
        assert isinstance(path, Path)
        assert str(path) == 'foo'

        # out of fans.path.Path
        path = Path(Path('foo'))
        assert isinstance(path, Path)
        assert str(path) == 'foo'
    
    def test_ensure_parent(self, tmp_path):
        # create if not exists
        path = Path(tmp_path / 'foo' / 'bar')
        path.ensure_parent()
        assert path.parent.exists()
        
        # it's ok if already exists
        path.ensure_parent()
        assert path.parent.exists()
        
        # create parents
        path = Path(tmp_path / 'a/b/c/d/e')
        path.ensure_parent()
        assert path.parent.exists()
    
    def test_ensure_dir(self, tmp_path):
        path = Path(tmp_path / 'foo' / 'bar')
        path.ensure_dir()
        assert path.is_dir()
    
    def test_ensure_file(self, tmp_path):
        path = Path(tmp_path / 'foo' / 'bar')
        path.ensure_file()
        assert path.is_file()
    
    def test_remove(self, tmp_path):
        path = Path(tmp_path / 'foo' / 'bar')

        # remove file
        path.ensure_file().remove()
        assert not path.exists()
        
        # remove dir (empty)
        path.ensure_dir().remove()
        assert not path.exists()
        
        # remove dir (non empty)
        path.ensure_dir()
        (path / 't.txt').ensure_file()
        path.remove()
        assert not path.exists()
