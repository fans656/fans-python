import pathlib
from types import SimpleNamespace
from typing import Iterable

import fans.tree.tree
import fans.bunch

from .enhanced import Path


def maketree(specs):
    assert isinstance(specs, Iterable), f"specs should be an iterable, not {type(specs)}"
    specs = list(normalize_specs(specs))
    root = fans.tree.make({'path': '', 'children': specs}, PathNode, assign_parent = True)
    root.data.path = Path('')
    root.children.normalize()
    root.derive()
    return root.build_namespace()


def normalize_specs(specs):
    def ensure_cur(cur, stage, token, stage_name = None):
        if not cur:
            raise ValueError(f"unexpected token: {token}")
        if stage in cur:
            raise ValueError(f"multiple {stage_name or stage} for {cur['path']}")

    def finalize(ret):
        if 'conf' in ret:
            conf = ret['conf']
            del ret['conf']
            ret.update(conf)
        return ret

    cur = {}
    for spec in specs:
        if isinstance(spec, (str, pathlib.Path)):
            if cur:
                yield finalize(cur)
            cur = {'path': spec}
        elif isinstance(spec, (set, dict)):
            ensure_cur(cur, 'conf', spec)
            cur['conf'] = normalize_conf(spec, cur['path'])
        elif isinstance(spec, list):
            ensure_cur(cur, 'children', spec, 'children list')
            cur['children'] = list(normalize_specs(spec))
        else:
            raise ValueError(f"invalid spec in path tree: {spec}")
    if cur:
        yield finalize(cur)


def normalize_conf(conf, path):
    if isinstance(conf, set):
        assert len(conf) == 1, f"invalid conf {conf} for {path}"
        conf = {'name': next(iter(conf))}
    assert isinstance(conf, dict), f"invalid conf {conf}"
    return conf


class PathNode:

    def __init__(self, data):
        self.name = data.get('name')
        self.path = data['path']

    def normalize(self):
        if isinstance(self.path, str) and self.path.startswith('~'):
            self.path = pathlib.Path.home() / self.path.lstrip('~/')

    def derive(self):
        self.path = self.parent.path / self.path

    def build_namespace(self):
        return NamespacedPath(self.path).with_namespace({
            node.name: node.build_namespace()
            for node in self.node.descendants if node.name
        })

    def __repr__(self):
        return f"{self.path} {self.name}"


class NamespacedPath(Path):

    def with_namespace(self, namespace):
        for name, value in namespace.items():
            if hasattr(self, name):
                raise ValueError(f"{name} is overriding attribute on {self}")
            setattr(self, name, value)
        return self
