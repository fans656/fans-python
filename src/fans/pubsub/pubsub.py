"""
Use cases
================================================================================

Replace callbacks passing

    # instead of doing this
    Storages(
        on_storage_added = ...,
        on_storage_removed = ...,
        on_file_uploaded = ...,
    )
    Storages.on_storage_added(...)
    Storages.on_storage_removed(...)
    S3Storage.on_file_uploaded(...)

    # you can do this
    pubsub.subscribe('storage.added', ...)
    pubsub.subscribe('storage.removed', ...)
    pubsub.subscribe('file.uploaded', ...)

    pubsub.publish('storage.added', ...)
    pubsub.publish('storage.removed', ...)
    pubsub.publish('file.uploaded', ...)

Collect events in SSE response generator

    @app.get('/api/events')
    async def events(request: Request):
        async def gen():
            async with pubsub.subscribe.async_events as events:
                while not await request.is_disconnected():
                    try:
                        yield await events.get(timeout = 0.02)
                    except asyncio.exceptions.TimeoutError:
                        pass
        return EventSourceResponse(gen())

Events consumer
================================================================================

    # subscribe for events (of everything)

    for event in subscribe():
        print(event)

    # subscribe for events of topic

    for event in subscribe('foo'):
        print(event)

Register callback

    # register callback for everything
    subscribe(callback)

    # register callback for topic
    subscribe('foo', callback)
"""
import queue
import asyncio
import traceback
from typing import Callable, Union
from collections import defaultdict


class PubSub:

    def __init__(self):
        self._topic_to_subs = defaultdict(lambda: set())

    def publish(
            self,
            topic: str,
            data: any,
    ):
        for _topic in nested_topics(topic):
            subs = self._topic_to_subs.get(_topic)
            if subs:
                for sub in tuple(subs):
                    try:
                        sub._invoke_callback(topic, data)
                    except:
                        traceback.print_exc()

    def subscribe(
            self,
            topic: str = '',
            callback: Callable[[any, str], None] = None,
    ) -> 'Subscription':
        if callable(topic):
            if callback is None:
                callback = topic
                topic = ''
            else:
                raise ValueError(f'invalid subscribe arguments: {(topic, callback)}')
        if topic == '*':
            topic = ''
        sub = Subscription(topic, callback, self)
        if callback:
            self._topic_to_subs[topic].add(sub)
        return sub

    def unsubscribe(
            self,
            token: Union['Subscription', callable],
            topic: str = '',
    ):
        subs = self._topic_to_subs.get(topic)
        if subs:
            subs.discard(token)

    def subscribed(
            self,
            token: Union['Subscription', callable],
            topic: str = '',
    ) -> bool:
        subs = self._topic_to_subs.get(topic)
        return subs and token in subs


class Subscription:

    def __init__(self, topic, callback, pubsub):
        self.topic = topic
        self.callback = callback
        self.pubsub = pubsub

        self.mode = None

    @property
    def events(self):
        if self.mode:
            raise RuntimeError(f'subscription already have mode set to {self.mode}')
        events = Events(self)
        self.callback = events.put_event
        self.pubsub._topic_to_subs[self.topic].add(self)
        self.mode = 'events'
        return iter(events)

    @property
    def async_events(self):
        if self.mode:
            raise RuntimeError(f'subscription already have mode set to {self.mode}')
        events = AsyncEvents(self)
        self.callback = events.put_event
        self.pubsub._topic_to_subs[self.topic].add(self)
        self.mode = 'async_events'
        return events

    def __enter__(self):
        pass

    def __exit__(self, *_):
        self.pubsub.unsubscribe(self, self.topic)

    def _invoke_callback(self, topic, data):
        self.callback(topic, data)

    def __hash__(self):
        return hash(self.callback)

    def __eq__(self, other):
        return self.callback is other or self.callback == other.callback


class Events:

    def __init__(self, sub):
        self.sub = sub
        self.queue = queue.Queue()

    def put_event(self, topic, data):
        self.queue.put((topic, data))

    def __iter__(self):
        while True:
            yield self.queue.get()


class AsyncEvents:

    def __init__(self, sub):
        self.sub = sub
        self.queue = self.make_janus_queue()
        self.closed = False

    def put_event(self, topic, data):
        if not self.closed:
            self.queue.sync_q.put((topic, data))

    async def get(self, timeout: int = None):
        if timeout:
            return await asyncio.wait_for(anext(self), timeout = timeout)
        else:
            return await anext(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_cls, exc, trace):
        self.sub.__exit__(exc_cls, exc, trace)
        await self.close()

    async def __await__(self):
        return self

    async def close(self):
        self.closed = True
        self.queue.close()
        await self.queue.wait_closed()

    def make_janus_queue(self):
        import janus
        return janus.Queue()

    async def collect_events(self):
        while True:
            yield await self.queue.async_q.get()

    async def __anext__(self):
        async for event in self.collect_events():
            return event

    def __aiter__(self):
        return aiter(self.collect_events())


def nested_topics(topic: str):
    yield topic
    while (index := topic.rfind('.')) != -1:
        topic = topic[:index]
        yield topic
    if topic:
        yield ''


pubsub = PubSub()
publish = pubsub.publish
subscribe = pubsub.subscribe
unsubscribe = pubsub.unsubscribe
