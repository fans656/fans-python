import hashlib
import traceback
import functools
from collections import defaultdict
from typing import Callable, Iterable, Tuple
from bisect import bisect_left, bisect_right

from fans.bunch import bunch
from fans.logger import get_logger
from fans.fn import calc_dict_md5, noop, empty_iter


logger = get_logger(__name__)


class Sync:

    def __init__(self, domain: str = '', **kwargs):
        self.domain = domain
        self.choose_item = kwargs.pop('choose_item', noop)
        self.batch_limit = kwargs.pop('batch_limit', 32)
        self.tree = MerkleTree(**kwargs)

    async def sio_sync(self, *args, **kwargs):
        await self.sync(SocketIOPeer(self.domain, *args, **kwargs))

    async def sync(self, peer: 'Peer'):
        await self.build_tree()
        await peer.build_tree()

        stack = ['']
        while stack:
            prefix = stack.pop()

            self_node = await self.get_node(prefix)
            peer_node = await peer.get_node(prefix)

            if self_node['hash_value'] == peer_node['hash_value']:
                continue

            if max(self_node['size'], peer_node['size']) <= self.batch_limit:
                await self.sync_batch(self_node, peer_node, peer)
            else:
                self_prefixes = set(self_node['child_prefixes'])
                peer_prefixes = set(peer_node['child_prefixes'])
                await self.sync_diff(self_prefixes, peer_prefixes, stack, peer)

    async def sync_batch(self, self_node, peer_node, peer):
        self_mapping = {d['uuid']: item for d in await self.get_items(self_node['prefix'])}
        peer_mapping = {d['uuid']: item for d in await self.get_items(peer_node['prefix'])}
        self_uuids = set(self_mapping.keys())
        peer_uuids = set(peer_mapping.keys())

        peer_missings = [self_mapping[uuid] for uuid in self_uuids - peer_uuids]
        if peer_missings:
            await peer.add_items(peer_missings)

        self_missings = [peer_mapping[uuid] for uuid in peer_uuids - self_uuids]
        if self_missings:
            await self.add_items(self_missings)

        self_updates = []
        peer_updates = []
        for uuid, self_item, peer_item in [
                (uuid, self_mapping[uuid], peer_mapping[uuid])
                for uuid in self_uuids & peer_uuids
        ]:
            chosen = self.select_newest_item(self_item, peer_item)
            if chosen is self_item:
                peer_updates.append((uuid, self_item))
            elif chosen is peer_item:
                self_updates.append((uuid, peer_item))
            else:
                logger.warning(
                    f'conflict item update where '
                    f'self = {self_item} and peer = {peer_item}'
                )
        if self_updates:
            await self.update_items(self_updates)
        if peer_updates:
            await peer.update_items(peer_updates)

    def select_newest_item(self, self_item, peer_item):
        self_mtime = max(
            self_item.get('__mtime__', 0),
            self_item.get('__dtime__', 0),
        ) or 0
        peer_mtime = max(
            peer_item.get('__mtime__', 0),
            peer_item.get('__dtime__', 0),
        ) or 0
        if self_mtime > peer_mtime:
            return self_item
        elif peer_mtime > self_mtime:
            return peer_item
        else:
            return self.choose_item(self_item, peer_item)

    async def sync_diff(self, self_prefixes, peer_prefixes, stack, peer):
        for prefix in self_prefixes - peer_prefixes:
            await self.sync_missings(self, peer, prefix)

        for prefix in peer_prefixes - self_prefixes:
            await self.sync_missings(peer, self, prefix)

        stack.extend(self_prefixes & peer_prefixes)

    async def sync_missings(self, src, dst, prefix):
        src_node = await src.get_node(prefix)
        for nodes in chunked(await src.get_items(prefix), self.batch_size):
            await dst.add_items(nodes)

    async def build_tree(self):
        self.tree.build()

    async def get_node(self, prefix):
        node = self.tree.get_node(prefix)
        return {
            'hash_value': node.hash_value,
            'prefix': node.prefix,
            'child_prefixes': [d.prefix for d in node.children],
            'data': node.data,
            'size': node.size,
        }

    async def add_items(self, nodes):
        for node in nodes:
            self.tree.add_item(node['uuid'], node['item'])

    async def update_items(self, nodes):
        for node in nodes:
            self.tree.update_item(node['uuid'], node['item'])

    async def get_items(self, prefix):
        return [{
            'uuid': d.prefix,
            'item': d.data,
            'hash_value': d.hash_value,
        } for d in self.tree.get_leaves(prefix)]

    async def handle_request(self, req):
        try:
            return {'data': await getattr(self, req['action'])(*req['args'])}
        except Exception as exc:
            return {'exception': str(exc), 'trace': traceback.format_exc()}

    @property
    def items(self):
        return self.tree.items


class Peer:

    async def build_tree(self, *args):
        return await self.__request_action('build_tree', args)

    async def get_node(self, *args):
        return await self.__request_action('get_node', args)

    async def add_items(self, *args):
        return await self.__request_action('add_items', args)

    async def update_items(self, *args):
        return await self.__request_action('update_items', args)

    async def get_items(self, args):
        return await self.__request_action('get_items', args)

    async def __request_action(self, action, args):
        return await self.request({'domain': self.domain, 'action': action, 'args': args})


def wrap_as_request(func):
    async def request(req):
        res = await func(req)
        if res.get('exception'):
            logger.error(res['trace'])
            raise RuntimeError(res['exception'])
        return res['data']
    return request


class SocketIOPeer(Peer):

    def __init__(self, domain, sio, topic = 'osync'):
        async def emit(req):
            return await sio.call(topic, req, timeout = 5)
        self.domain = domain
        self.request = wrap_as_request(emit)


class MerkleTree:

    def __init__(
            self,
            iter_items: Callable[[], Iterable[Tuple[
                str, # guid of the item
                dict, # item data
                int, # size of the item, optional and defaults to 1
            ]]] = empty_iter,
            add_item: Callable[[
                str, # guid of the item
                dict, # item data
            ], None] = noop,
            update_item: Callable[[
                str, # guid of the item
                dict, # item data
            ], None] = noop,
    ):
        self.add_item = add_item
        self.update_item = update_item
        self.iter_items = iter_items

    @property
    def items(self):
        for _, data, __ in self.iter_items():
            yield data

    def get_leaves(self, prefix) -> list:
        node = self.get_node(prefix)
        if not node:
            return []
        padding_len = 32 - len(prefix)
        prefix_beg = prefix + '0' * padding_len
        prefix_end = prefix + 'f' * padding_len
        index_beg = bisect_left(self.leaves, prefix_beg, key = lambda d: d.prefix)
        index_end = bisect_right(self.leaves, prefix_end, key = lambda d: d.prefix)
        return self.leaves[index_beg:index_end]

    def get_node(self, prefix):
        cur = self.root
        for i in range(len(prefix)):
            cur = cur.prefix_to_child.get(prefix[:i+1])
            if not cur:
                return None
        return cur

    def build(self):
        leaves = []
        for guid, data, size in self.iter_items():
            leaves.append(TreeNode(
                prefix = guid,
                data = data,
                hash_value = calc_dict_md5(data),
                size = size or 1,
            ))
        leaves.sort(key = lambda d: d.prefix)

        if not leaves:
            self.root = TreeNode()
        else:
            levels = [leaves]
            while len(levels[-1]) > 1:
                children = levels.pop()
                parents = []
                index = 0
                while index < len(children):
                    child = children[index]
                    prefix = child.prefix
                    prefix_beg = prefix[:-1] + '0'
                    prefix_end = prefix[:-1] + 'f'
                    index_beg = bisect_left(children, prefix_beg, key = lambda d: d.prefix)
                    index_end = bisect_right(children, prefix_end, key = lambda d: d.prefix)
                    group = children[index_beg:index_end]

                    hasher = hashlib.md5()
                    for node in group:
                        hasher.update(node.hash_value.encode())
                    parents.append(TreeNode(
                        prefix = prefix[:-1],
                        hash_value = hasher.hexdigest(),
                        size = sum(d.size for d in group),
                        children = group,
                    ))

                    index = index_end

                levels.append(parents)

            self.root = levels[-1][0]

        self.leaves = leaves


class TreeNode:

    def __init__(self, prefix = '', data = None, hash_value = None, size = 0, children = []):
        self.prefix = prefix
        self.data = data
        self.hash_value = hash_value
        self.size = size
        self.children = children
        self.prefix_to_child = {d.prefix: d for d in children}
