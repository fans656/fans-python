import pathlib
from typing import Iterable, List, Union

import fans.tree.tree
import fans.bunch

from fans.path.enhanced import Path


def make_paths(root: Union[str, pathlib.Path], specs: Iterable[any] = None) -> 'NamespacedPath':
    """
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
    if specs is None:
        specs = root
        root = ''
    root = make_specs_tree(root, specs)
    return root.node.build_namespace()


def make_specs_tree(root_path, specs: Iterable) -> 'PathNode':
    assert isinstance(specs, Iterable), f"specs should be an iterable, not {type(specs)}"
    specs = list(normalize_specs(specs))
    root = fans.tree.make({'path': '', 'children': specs}, PathNode, assign_parent = True)
    root.data.path = Path(root_path)
    root.children.normalize()
    root.derive()
    return root


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
            cur.update(normalize_conf(spec, cur['path']))
        elif isinstance(spec, list):
            ensure_cur(cur, 'children', spec, 'children list')
            cur['children'] = list(normalize_specs(spec))
        else:
            raise ValueError(f"invalid spec in path tree: {repr(spec)}")
    if cur:
        yield cur


def normalize_conf(conf, path):
    if isinstance(conf, set):
        assert len(conf) == 1, f"invalid conf {conf} for {path}"
        conf = {'name': next(iter(conf))}
    assert isinstance(conf, dict), f"invalid conf {conf}"
    return conf


class PathNode:

    def __init__(self, data):
        self.data = data
        self.name = data.get('name')
        self.path = data['path']

    def normalize(self):
        if isinstance(self.path, str) and self.path.startswith('~'):
            self.path = pathlib.Path.home() / self.path.lstrip('~/')

    def derive(self):
        self.path = self.parent.path / self.path
        if self.data.get('create') == 'dir':
            self.path.ensure_dir()

    def build_namespace(self) -> 'NamespacedPath':
        return NamespacedPath(self.path).with_namespace({
            node.name: node.build_namespace()
            for node in self.node.descendants if node.name
        }).with_node(self)

    def create(self):
        if 'children' in self.data:
            self.path.ensure_dir()
        else:
            self.path.touch()
        for child in self.node.children:
            child.create()

    def as_dict(self):
        return {
            'path': str(self.path),
            'children': sorted(
                [c.as_dict() for c in self.node.children],
                key = lambda d: d['path'],
            ),
        }

    def __repr__(self):
        return f"{self.__class__.__name__}(path = {self.path}, name = {self.name})"


class NamespacedPath(Path):

    def create(self) -> 'NamespacedPath':
        """
        Create file/dir structure for this path tree.
        """
        self._node.create()
        return self

    def with_namespace(self, namespace: dict) -> 'NamespacedPath':
        for name, value in namespace.items():
            if hasattr(self, name):
                raise ValueError(f"{name} is overriding attribute on {repr(self)}")
            setattr(self, name, value)
        return self

    def with_node(self, node: PathNode) -> 'NamespacedPath':
        self._node = node
        return self

    def as_dict(self):
        return self._node.as_dict()

    def __iter__(self):
        for node in self._node.node.descendants:
            yield node


if __name__ == '__main__':
    import doctest
    doctest.testmod()
