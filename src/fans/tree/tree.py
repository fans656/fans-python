import functools
from typing import Callable, Any, Iterable, List

from fans.fn import noop, identity
from fans.bunch import bunch
from fans.vectorized import vectorized


class Node:
    """
    Represent a node in the tree.

    Each node has field:
        data - the underlying data
        parent - the node's parent, None if no parent
        children - a list of children Node
    """

    def __init__(self, data, parent = None):
        self.data = data
        self.parent = parent
        self._children = None

    @property
    @vectorized
    def children(self):
        for child in self._children:
            yield child.data

    @property
    @vectorized
    def nodes(self):
        yield self.data
        for child in self._children:
            yield from child.nodes

    @property
    @vectorized
    def descendants(self):
        for child in self._children:
            yield from child.nodes

    @property
    @vectorized
    def leaves(self):
        if self._children:
            for child in self._children:
                yield from child.leaves
        else:
            yield self.data

    def derive(self, func = None, ensure_parent = True, derive_args = (), derive_kwargs = {}):
        if func is None:
            func = lambda data: getattr(data, 'derive')(*derive_args, **derive_kwargs)
        elif isinstance(func, str):
            key = func
            func = lambda data: getattr(data, key)(*derive_args, **derive_kwargs)
        self._derive(func, ensure_parent = ensure_parent)

    def _derive(self, func, ensure_parent = True):
        if not ensure_parent or self.parent:
            func(self.data)
        self._children._derive(func, ensure_parent = False)

    def __getattr__(self, key):
        return getattr(self.data, key)

    def show(self, fmt = str, depth = 0):
        indent = '  ' * depth
        print(f"{indent}{fmt(self.data)}")
        self._children.show(fmt = fmt, depth = depth + 1)


NodeData = Any
WrappedNodeData = object
GetChildren = Callable[[NodeData], Iterable[NodeData]]
AssignNode = Callable[[WrappedNodeData, Node], None]
AssignParent = Callable[[WrappedNodeData, Node], None]
AssignChildren = Callable[[WrappedNodeData, List[Node]], None]
Wrap = Callable[[NodeData], WrappedNodeData]


def normalize_get_children(spec):
    if isinstance(spec, str):
        return lambda data: data.get(spec) or []
    elif callable(spec):
        return spec
    else:
        raise ValueError(f"invalid spec for get children: {spec}")


def normalize_assign_node(spec) -> AssignNode:
    if isinstance(spec, str):
        return lambda data, node: setattr(data, spec, node)
    elif spec is True:
        return lambda data, node: setattr(data, 'node', node)
    elif callable(spec):
        return spec
    elif spec is None:
        return noop
    else:
        raise ValueError(f"invalid spec for assign node: {spec}")


def normalize_assign_parent(spec) -> AssignParent:
    if spec is None:
        return noop
    elif spec is True:
        return lambda data, parent: setattr(data, 'parent', parent)
    elif isinstance(spec, str):
        return lambda data, parent: setattr(data, spec, parent)
    elif callable(spec):
        return spec
    else:
        raise ValueError(f"invalid spec for assign parent: {spec}")


def normalize_assign_children(spec) -> AssignChildren:
    if spec is None:
        return noop
    elif spec is True:
        return lambda data, children: setattr(data, 'children', children)
    elif isinstance(spec, str):
        return lambda data, children: setattr(data, spec, children)
    elif callable(spec):
        return spec
    else:
        raise ValueError(f"invalid spec for assign children: {spec}")


class TreeMaker:

    def __init__(
            self,
            get_children: GetChildren,
            assign_node: AssignNode,
            assign_parent: AssignParent,
            assign_children: AssignChildren,
            wrap: Wrap,
            node_cls: Node,
    ):
        assert issubclass(node_cls, Node), f"node_cls must be subclass of Node"
        self.get_children = get_children
        self.assign_node = assign_node
        self.assign_parent = assign_parent
        self.assign_children = assign_children
        self.wrap = wrap
        self.node_cls = node_cls

    def make_node(self, data, parent = None):
        node = self.node_cls(self.wrap(data), parent = parent)
        node._children = vectorized([self.make_node(d, node) for d in self.get_children(data)])
        self.assign_node(node.data, node)
        self.assign_parent(node.data, parent.data if parent else None)
        self.assign_children(node.data, node.children)
        return node


def make(
        data,
        wrap = bunch,
        children = 'children',
        assign_node = 'node',
        assign_parent = None,
        assign_children = None,
        node_cls = Node,
) -> Node:
    """
    Make a tree structure out of given data.

    Args:

    Returns:
        Node
    """
    return TreeMaker(
        get_children = normalize_get_children(children),
        assign_node = normalize_assign_node(assign_node),
        assign_parent = normalize_assign_parent(assign_parent),
        assign_children = normalize_assign_children(assign_children),
        wrap = wrap,
        node_cls = node_cls,
    ).make_node(data)
