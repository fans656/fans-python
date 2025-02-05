import asyncio
import threading

import pytest

from fans.pubsub1 import pubsub, PubSub


def test_single_thread():
    pubsub = PubSub()
    collector = Collector(limit = 1, pubsub = pubsub)
    collector.consumer = pubsub.subscribe(collector)
    pubsub.publish('foo')
    pubsub.run()
    assert collector.collected == ['foo']


def test_single_thread_multi_consumers():
    pubsub = PubSub()
    collector1 = Collector(limit = 1, pubsub = pubsub)
    collector2 = Collector(limit = 1, pubsub = pubsub)
    collector1.consumer = pubsub.subscribe(collector1)
    collector2.consumer = pubsub.subscribe(collector2)
    pubsub.publish('foo')
    pubsub.run()
    assert collector1.collected == ['foo']
    assert collector2.collected == ['foo']


def test_multi_thread():
    pubsub = PubSub()
    collector = Collector(limit = 1, pubsub = pubsub)

    def threaded():
        collector.consumer = pubsub.subscribe(collector)
        pubsub.run()

    thread = threading.Thread(target = threaded)
    thread.start()
    pubsub.publish('foo')
    thread.join()
    assert collector.collected == ['foo']


@pytest.mark.asyncio
async def test_async_subscribe():
    pubsub = PubSub()
    collector = Collector(limit = 1, pubsub = pubsub)
    collector.consumer = await pubsub.subscribe_async(collector)
    threading.Thread(target = lambda: pubsub.publish('foo')).start()
    await pubsub.run_async()
    assert collector.collected == ['foo']


@pytest.mark.asyncio
async def test_async_without_callback():
    pubsub = PubSub()
    with await pubsub.subscribe_async() as events:
        threading.Thread(target = lambda: pubsub.publish('foo')).start()
        event = await events.get_async()
        assert event == 'foo'
    await pubsub.join_async()


@pytest.mark.asyncio
async def test_async_nowait():
    pubsub = PubSub()
    with await pubsub.subscribe_async() as events:
        with pytest.raises(asyncio.QueueEmpty):
            event = await events.get_async(wait = False)
    await pubsub.join_async()


@pytest.mark.asyncio
async def test_async_without_callback_multi_consumers():
    pubsub = PubSub()
    async def consume():
        with await pubsub.subscribe_async() as events:
            assert await events.get_async() == 'foo'
    async def produce():
        pubsub.publish('foo')
    task1 = asyncio.create_task(consume())
    task2 = asyncio.create_task(consume())
    await asyncio.gather(task1, task2, produce())


class Collector:

    def __init__(self, limit = 0, pubsub = pubsub):
        self.pubsub = pubsub
        self.collected = []
        self.consumer = None
        self.limit = limit

    def __call__(self, data):
        self.collected.append(data)
        if self.limit and len(self.collected) == self.limit:
            self.pubsub.unsubscribe(self.consumer)
