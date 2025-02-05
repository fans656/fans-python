"""
Utility to ease the defining of multiple paths (files/directories) for use.

We can define a hierachy of file structure like this:

    paths = make_paths(Path(__file__).parent, [
        'data', [
            'conf', [
                'conf.yaml', {'conf'},
                'jobs.yaml', {'jobs'},
            ],
            'cache', [
                'assets.sqlite', {'assets_cache'},
            ],
        ],
        'temp', {'temp'}, [],
    ])

then reference them easily:

    paths.conf          # <root>/data/conf/conf.yaml
    paths.jobs          # <root>/data/conf/jobs.yaml
    paths.assets_cache  # <root>/data/cache/assets.sqlite
    paths.temp          # <root>/temp/
"""
import pathlib
from functools import reduce
from typing import Iterable, List, Union, Optional

from fans.tree import make as make_tree
from fans.path.enhanced import Path


def make_paths(*args) -> 'NamespacedPath':
    """
    Make a paths tree.

    Usage:

        # relative paths
        make_paths([
            'foo.txt', {'foo'},
        ])

        # absolute paths
        make_paths('/tmp', [
            'foo.txt', {'foo'},
        ])

        # absolute paths with root conf
        make_paths('/tmp/hello', {'create': 'dir'}, [
            'foo.txt', {'foo'},
        ])

    >>> paths = make_paths([
    ...    'temp', [
    ...        'foo.yaml', {'foo'},
    ...        'bar.yaml', {'bar'},
    ...    ],
    ...    'baz.json', {'baz'},
    ... ])

    >>> paths.foo
    NamespacedPath('temp/foo.yaml')
    >>> paths.bar
    NamespacedPath('temp/bar.yaml')
    >>> paths.baz
    NamespacedPath('baz.json')


    >>> make_paths('/tmp', [
    ...     'test.txt', {'test'},
    ... ]).test
    NamespacedPath('/tmp/test.txt')
    """
    root_path, root_conf, children_specs = normalize_args(args)
    root = make_tree({
        **root_conf,
        'path': root_path,
        'children': children_specs,
    }, wrap = Node, assign_parent = True)
    root.children.normalize()
    root.derive()
    root.derive('make', ensure_parent = False)
    root.derive('build', bottomup = True)

    return root.data.path


def normalize_args(args):
    invalid_args = False

    if len(args) == 1 and isinstance(args[0], list):  # make_paths(['foo.txt', {'foo'}])
        root = ''
        conf = {}
        specs = args[0]
    elif len(args) == 2:  # make_paths('/tmp', ['foo.txt', {'foo'}])
        if isinstance(args[1], list):
            root = Path(args[0])
            conf = {}
            specs = args[1]
        elif isinstance(args[1], (set, dict)):
            root = Path(args[0])
            conf = args[1]
            specs = []
        else:
            invalid_args = True
    elif (
        len(args) == 3
        and isinstance(args[1], (set, dict))
        and isinstance(args[2], list)
    ):
        root = args[0]
        conf = args[1]
        specs = args[2]
    else:
        invalid_args = True

    if invalid_args:
        raise RuntimeError(f'invalid arguments: {args}')

    assert isinstance(specs, Iterable), f'specs should be an iterable, not {type(specs)}'
    specs = list(normalize_specs(specs))
    conf = normalize_conf(conf)

    return Path(root), conf, specs


def normalize_specs(specs: Iterable) -> List[dict]:
    def ensure_cur(cur, stage, token, stage_name = None):
        if not cur:
            raise ValueError(f"unexpected token: {token}")
        if stage in cur:
            raise ValueError(f"multiple {stage_name or stage} for {cur['path']}")

    cur = {}
    for spec in specs:
        if isinstance(spec, (str, pathlib.Path, pathlib.PurePath)):
            if cur:
                yield cur
            cur = {'path': spec}
        elif isinstance(spec, (set, dict)):
            ensure_cur(cur, 'conf', spec)
            cur.update(normalize_conf(spec))
        elif isinstance(spec, list):
            ensure_cur(cur, 'children', spec, 'children list')
            cur['children'] = list(normalize_specs(spec))
        else:
            raise ValueError(f"invalid spec in path tree: {repr(spec)}")
    if cur:
        yield cur


def normalize_conf(conf):
    """
    Conf fields: {
        name: str - name of the path
        create: str - ensure the path exists as given type ("dir" | "file")
    }

    You can also use a set {'foo'}, which is equivalent to {'name': 'foo'}.
    """
    if isinstance(conf, set):
        assert len(conf) == 1, f"invalid conf {conf} for {path}"
        conf = {'name': next(iter(conf))}
    assert isinstance(conf, dict), f"invalid conf {conf}"
    return conf


class NamespacedPathImpl:

    def create(self):
        raise NotImplementedError()

    def with_tree(self):
        raise NotImplementedError()

    def with_root(self):
        raise NotImplementedError()


class Node(NamespacedPathImpl):

    def __init__(self, data: dict):
        self.data = data
        self.name = data.get('name')
        self.path = data['path']
        self.name_to_path = {}

    def normalize(self):
        # expand user home
        if isinstance(self.path, str) and self.path.startswith('~'):
            self.path = pathlib.Path.home() / self.path.lstrip('~/')

    def derive(self):
        # normalize to "absolute" path from parent path
        self.path = self.parent.path / self.path

    def make(self):
        self.path = NamespacedPath(self.path)._with_impl(self)
        if self.name:
            self.name_to_path[self.name] = self.path

        if self.data.get('create') == 'dir':
            self.path.ensure_dir()

    def change_root(self, old_root, new_root):
        self.path = new_root / self.path.relative_to(old_root)
        self.data['path'] = str(self.path)

    def build(self, target: 'Node' = None) -> 'NamespacedPath':
        """
        Set attributes (key -> path) from given target or self.
        """
        for name, path in self.name_to_path.items():
            setattr(self.path, name, path)

        for name, path in reduce(
                lambda acc, x: {**acc, **x},
                (target or self).node.children.name_to_path,
                {},
        ).items():
            self.name_to_path[name] = path
            setattr(self.path, name, path)
        return self

    # @override
    def create(self):
        if 'children' in self.data or self.data.get('create') == 'dir':
            self.path.ensure_dir()
        else:
            if (content := self.data.get('content')):
                if isinstance(content, pathlib.Path):
                    with content.open('rb') as f:
                        content = f.read()
                with self.path.open('w' if isinstance(content, str) else 'wb') as f:
                    f.write(content)
            elif self.data.get('create') == 'file':
                self.path.touch()
            else:
                # NOTE: do not create file if not explicitly specified
                # e.g. it's not suitable to create an empty `data.sqlite` file
                pass
        self.node.children.create()

    # @override
    def with_tree(self, specs):
        root = make_paths(self.path, specs)
        self.build(root._impl)
        self.node.root.data.build(root._impl)

    # @override
    def with_root(self, root):
        self.node.root.derive(
            'change_root',
            derive_args=(self.node.root.path, Path(root),),
            ensure_parent=False,
        )
        self.node.root.derive('make', ensure_parent=False)
        self.node.root.derive('build', bottomup=True)
        return self.node.root.path


class NamespacedPath(Path):

    def create(self):
        self._impl.create()
        return self

    def with_tree(self, specs):
        """
        Attach the tree given by `specs` to current path. Root namespace is also updated.

        paths = make_paths([
            'core', {'core'},
        ])
        paths.core.with_tree([
            'fs.sqlite', {'database_path'},
        ])
        assert paths.database_path == Path('core/fs.sqlite')
        """
        self._impl.with_tree(specs)
        return self

    def with_root(self, root):
        return self._impl.with_root(root)

    def _with_impl(self, impl: NamespacedPathImpl):
        self._impl = impl
        return self


if __name__ == '__main__':
    import doctest
    doctest.testmod()
