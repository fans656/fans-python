from pathlib import Path

from fans.path import make_paths


class Test_make_paths:

    def test_default(self):
        paths = make_paths([
            'somedir', [
                'foo.txt', {'foo'},
            ],
            'bar.txt', {'bar'},
        ])
        assert paths.foo == Path('somedir/foo.txt')
        assert paths.bar == Path('bar.txt')

    def test_create(self, tmpdir):
        paths = make_paths(tmpdir, [
            'foo.txt', {'foo'},
        ])
        assert not paths.foo.exists()
        paths.create()
        assert paths.foo.exists()

    def test_auto_create(self, tmpdir):
        paths = make_paths(tmpdir, [
            'foo', {'name': 'foo', 'create': 'dir'},
        ])
        assert paths.foo.exists() and paths.foo.is_dir()

    def test_with_tree(self):
        paths = make_paths([
            'core', {'core'}, [
            ],
        ])
        paths.core.with_tree([
            'fs.sqlite', {'database_path'}
        ])
        assert paths.core
        assert paths.database_path == Path('core/fs.sqlite')


class Test_arguments:

    def test_no_root(self):
        assert make_paths(['foo.txt', {'foo'}]).foo

    def test_root_without_conf(self):
        assert make_paths('/tmp', ['foo.txt', {'foo'}]).foo

    def test_root_with_conf(self, tmpdir):
        root_path = tmpdir / 'asdf'
        assert make_paths(root_path, {'create': 'dir'}, ['foo.txt', {'foo'}]).foo
        assert root_path.exists()
