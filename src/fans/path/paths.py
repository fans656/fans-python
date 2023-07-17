import pathlib
from functools import reduce
from typing import Iterable, List, Union, Optional

import fans.tree.tree
import fans.bunch

from fans.path.enhanced import Path


def make_paths(
        root: Optional[Union[str, pathlib.Path]],
        conf: Optional[dict] = None,
        specs: Iterable[any] = None,
) -> 'NamespacedPath':
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
    if conf is None and specs is None: # make_paths(['foo.txt', {'foo'}])
        specs = root
        conf = {}
        root = ''
    elif specs is None: # make_paths('/tmp', ['foo.txt', {'foo'}])
        specs = conf
        conf = {}
    else: # make_paths('/tmp', {'create': 'dir'}, ['foo.txt', {'foo'}])
        pass

    assert isinstance(specs, Iterable), f"specs should be an iterable, not {type(specs)}"
    specs = list(normalize_specs(specs))
    root = fans.tree.make(
        {
            **normalize_conf(conf),
            'path': Path(root),
            'children': specs,
        },
        wrap = Node,
        assign_parent = True,
    )
    root.children.normalize()
    root.derive()
    root.derive('make', ensure_parent = False)
    root.derive('build', bottomup = True)

    return root.data.path


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


class Node:

    def __init__(self, data: dict):
        """
        data fields: {
            **conf,
            path: str - relative path of the node
            children: str - children data
        }
        """
        self.data = data
        self.name = data.get('name')
        self.path = data['path']
        self.name_to_path = {}

    def normalize(self):
        if isinstance(self.path, str) and self.path.startswith('~'):
            self.path = pathlib.Path.home() / self.path.lstrip('~/')

    def derive(self):
        self.path = self.parent.path / self.path

    def make(self):
        self.path = NamespacedPath(self.path)._with_impl(self)
        if self.name:
            self.name_to_path[self.name] = self.path

        if self.data.get('create') == 'dir':
            self.path.ensure_dir()

    def build(self, target: 'Node' = None) -> 'NamespacedPath':
        for name, path in reduce(
                lambda acc, x: {**acc, **x},
                (target or self).node.children.name_to_path,
                {},
        ).items():
            self.name_to_path[name] = path
            setattr(self.path, name, path)
        return self

    def create(self):
        if 'children' in self.data:
            self.path.ensure_dir()
        else:
            if (content := self.data.get('content')):
                with self.path.open('w') as f:
                    f.write(content)
            else:
                self.path.touch()
        self.node.children.create()

    def with_tree(self, specs):
        root = make_paths(self.path, specs)
        self.build(root._impl)
        self.node.root.data.build(root._impl)


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

    def _with_impl(self, impl):
        self._impl = impl
        return self


if __name__ == '__main__':
    import doctest
    doctest.testmod()
