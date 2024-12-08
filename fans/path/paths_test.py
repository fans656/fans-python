from pathlib import Path

from fans.path import make_paths


class TestUsage:

    def test_simple(self, tmp_path):
        """
        Given a root path and tree hierachy, can reference file/directory by key.
        """
        paths = make_paths(tmp_path, [
            'foo', [
                'bar.txt', {'bar'},
            ],
            'baz.txt', {'baz'},
        ])
        assert paths.bar == tmp_path / 'foo/bar.txt'
        assert paths.baz == tmp_path / 'baz.txt'

    def test_relative(self):
        """
        Given tree hierachy only, the paths are relative.
        """
        paths = make_paths([
            'foo', [
                'bar.txt', {'bar'},
            ],
            'baz.txt', {'baz'},
        ])
        assert paths.bar == Path('foo/bar.txt')
        assert paths.baz == Path('baz.txt')


class TestFeature:

    def test_expand_user_home(self):
        paths = make_paths([
            '~', {'home'},
            '~/.ssh', {'ssh'},
        ])
        assert paths.home == Path.home()
        assert paths.ssh == Path.home() / '.ssh'

    def test_with_root(self):
        paths = make_paths('root1', [
            'foo', {'foo'},
        ])
        assert paths.foo == Path('root1/foo')
        paths = paths.with_root('root2')
        assert paths.foo == Path('root2/foo')


class TestDetails:

    def test_root_key(self, tmp_path):
        """
        Root path can also be given a key.
        """
        paths = make_paths(tmp_path, {'foo'})
        assert paths.foo == tmp_path


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

    def test_create_file(self, tmpdir):
        paths = make_paths(tmpdir, [
            'foo.txt', {'name': 'foo', 'create': 'file'},
        ])
        assert not paths.foo.exists()
        paths.create()
        assert paths.foo.exists()

    def test_create_dir(self, tmpdir):
        paths = make_paths(tmpdir, [
            'foo', {'name': 'foo', 'create': 'dir'},
        ])
        assert paths.foo.exists() and paths.foo.is_dir()

    def test_create_tree(self, tmpdir):
        paths = make_paths(tmpdir, [
            'foo', {'foo'}, [
                'bar', {'bar'}, [],
                'baz', {'baz'}, [],
            ],
        ])
        paths.create()
        assert paths.foo.is_dir()
        assert paths.bar.is_dir()
        assert paths.baz.is_dir()

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
