import functools
from typing import Callable, Any, Iterable, List

from fans.fn import noop, identity
from fans.bunch import bunch
from fans.vectorized import vectorized


NodeData = Any
WrappedNodeData = object


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
    def root(self):
        cur = self
        while cur.parent:
            cur = cur.parent
        return cur

    @property
    @vectorized
    def children(self):
        """
        Note: this yields data instead of node itself, use `_children` for nodes
        """
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

    def derive(
            self,
            func = None,
            *,
            derive_args = (),
            derive_kwargs = {},
            ensure_parent = True,
            ensure_children = True,
            bottomup = False,
    ):
        # call data.derive(...)
        if func is None:
            _func = lambda data: getattr(data, 'derive')(*derive_args, **derive_kwargs)
        # call getattr(data, func)(...) where func is str
        elif isinstance(func, str):
            key = func
            _func = lambda data: getattr(data, key)(*derive_args, **derive_kwargs)
        # call func(data, ...)
        elif callable(func):
            _func = lambda data: func(data, *derive_args, **derive_kwargs)
        else:
            raise ValueError(f'invalid derive func "{func}"')

        if bottomup:
            self._derive_bottomup(_func, ensure_children = ensure_children)
        else:
            self._derive_topdown(_func, ensure_parent = ensure_parent)

    def _derive_topdown(self, func: Callable[[NodeData], None], ensure_parent: bool = True):
        """
        Params:
            func - callable to apply to each node
            ensure_parent - only call `func` when current node has parent
        """
        if not ensure_parent or self.parent:
            func(self.data)
        self._children._derive_topdown(func, ensure_parent = False)

    def _derive_bottomup(self, func: Callable[[NodeData], None], ensure_children: bool = True):
        """
        Params:
            func - callable to apply to each node
            ensure_children - only call `func` when current node has children
        """
        if self._children:
            self._children._derive_bottomup(func, ensure_children = ensure_children)
        if not ensure_children or self.children:
            func(self.data)

    def __getattr__(self, key):
        return getattr(self.data, key)

    def show(self, fmt = str, depth = 0):
        indent = '  ' * depth
        print(f"{indent}{fmt(self.data)}")
        self._children.show(fmt = fmt, depth = depth + 1)


GetChildren = Callable[[NodeData], Iterable[NodeData]]
AssignNode = Callable[[WrappedNodeData, Node], None]
ParentNode = Node
AssignParent = Callable[[WrappedNodeData, ParentNode], None]
AssignChildren = Callable[[WrappedNodeData, List[Node]], None]
Wrap = Callable[[NodeData], WrappedNodeData]


def normalize_get_children(spec):
    # data[spec] -> children data list
    if isinstance(spec, str):
        return lambda data: data.get(spec) or []
    # custom callable to get children data list
    elif callable(spec):
        return spec
    else:
        raise ValueError(f"invalid spec for get children: {spec}")


def normalize_assign_node(spec) -> AssignNode:
    # data.<spec> become the node
    if isinstance(spec, str):
        return lambda data, node: setattr(data, spec, node)
    # data.node become the node
    elif spec is True:
        return lambda data, node: setattr(data, 'node', node)
    # custom function to assign node attribute
    elif callable(spec):
        return spec
    # do not set node attribute
    elif not spec:
        return noop
    else:
        raise ValueError(f"invalid spec for assign node: {spec}")


def normalize_assign_parent(spec) -> AssignParent:
    # getattr(data, spec) -> parent
    if isinstance(spec, str):
        return lambda data, parent: setattr(data, spec, parent)
    # custom callable to assign parent attribute
    elif callable(spec):
        return spec
    # data.parent -> parent
    elif spec == True:
        return lambda data, parent: setattr(data, 'parent', parent)
    # do not assign parent
    elif not spec:
        return noop
    else:
        raise ValueError(f"invalid spec for assign parent: {spec}")


def normalize_assign_children(spec) -> AssignChildren:
    # no children attribute
    if spec is None:
        return noop
    # data.children -> children
    elif spec is True:
        return lambda data, children: setattr(data, 'children', children)
    # getattr(children, spec) -> children
    elif isinstance(spec, str):
        return lambda data, children: setattr(data, spec, children)
    # custom callable to assign children attribute
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

        try:
            self.assign_node(node.data, node)
        except AttributeError:
            pass

        try:
            self.assign_parent(node.data, parent.data if parent else None)
        except AttributeError:
            pass

        try:
            self.assign_children(node.data, node.children)
        except AttributeError:
            pass

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
