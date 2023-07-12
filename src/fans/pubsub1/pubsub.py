import queue
import asyncio
import traceback
import threading
from typing import Union, Callable

import janus


class PubSub:
    """
    Get pubsub instance:

        # option 1: use the pre-defined instance
        from fans.pubsub import pubsub

        # option 2: instantiate separate instance
        from fans.pubsub import PubSub
        pubsub = PubSub()

    different `PubSub` instances are separted environment for events publishing/subscribing.


    Usage (single thread):

        def callback(data):
            print(data)

        pubsub.subscribe(callback, 'foo') # registered the callback
        pubsub.publish({'hello': 'world'}, 'foo') # published the event into pubsub
        pubsub.run() # this will trigger the callback

    the `topic` parameter can be ignored and will defaults to `None`.
    """

    def __init__(self):
        self.running = False
        self._topic_to_consumers = {}
        self._topic_to_consumers_lock = threading.Lock()
        self._thread_id_to_runner = {}
        self._thread_id_to_runner_lock = threading.Lock()

    def publish(self, data, topic = None):
        consumers = self.get_consumers(topic)
        for consumer in consumers:
            consumer.publish(data)

    def subscribe(self, callback = None, topic = None, is_async = False) -> 'Consumer':
        consumer = Consumer(callback = callback, topic = topic, is_async = is_async)
        self.runner.add_consumer(consumer)
        self.get_consumers(topic).add(consumer)
        return consumer

    async def subscribe_async(self, callback = None, topic = None) -> 'Consumer':
        consumer = self.subscribe(callback, topic, is_async = True)
        asyncio.create_task(self.run_async())
        return consumer

    def unsubscribe(self, consumer: 'Consumer'):
        consumers = self.get_consumers(consumer.topic)
        consumers.discard(consumer)

        runner = consumer.runner
        runner.discard_consumer(consumer)
        if not runner.consumers:
            runner.stop()
            with self._thread_id_to_runner_lock:
                del self._thread_id_to_runner[runner.thread_id]

    def start(self):
        """
        Note: Call this only when used for blocking run.
        """
        self.thread = threading.Thread(target = self.run)
        self.thread.start()

    def run(self):
        """
        Blocking loop to run callbacks in current thread.

        Note: Call this only when used for blocking run.
        """
        # TODO: move while loop into runner
        if self.running:
            return
        runner = self.runner

        self.running = True

        while (event := runner.get_event()):
            consume, data = event
            consume(data)

        self.running = False

    async def run_async(self):
        if self.running:
            return
        runner = self.runner
        runner.make_async()
        if not runner.consumers:
            with self._thread_id_to_runner_lock:
                del self._thread_id_to_runner[runner.thread_id]
            return

        self.running = True

        while (event := await runner.get_event_async()):
            consumer, data = event
            await consumer.handle_async(data)
        await runner.close()

        self.running = False

    async def join_async(self):
        """
        Wait until all running loops to finish.
        """
        await asyncio.gather(*(asyncio.all_tasks() - {asyncio.current_task()}))

    @property
    def runner(self):
        thread_id = threading.get_ident()
        if thread_id not in self._thread_id_to_runner:
            with self._thread_id_to_runner_lock:
                self._thread_id_to_runner[thread_id] = Runner(pubsub = self)
        return self._thread_id_to_runner[thread_id]

    def get_consumers(self, topic):
        if topic not in self._topic_to_consumers:
            with self._topic_to_consumers_lock:
                self._topic_to_consumers[topic] = SetWithLock()
        return self._topic_to_consumers[topic]


class Consumer:

    def __init__(self, callback, topic, is_async = False):
        self.callback = callback
        self.topic = topic
        self.is_async = is_async

        self.runner = None
        self._orig_callback = None

        if is_async:
            if not asyncio.iscoroutinefunction(self.callback):
                self._orig_callback = self.callback
                self.callback = self.async_callback

        if not callback:
            self.callback = self.enqueue_data_callback
            self._data_queue = janus.Queue()

    def __enter__(self):
        return self

    def __exit__(self, *_, **__):
        self.runner.pubsub.unsubscribe(self)

    async def get_async(self, wait = True):
        if wait:
            return await self._data_queue.async_q.get()
        else:
            return self._data_queue.async_q.get_nowait()

    def publish(self, data):
        self.runner.publish((self, data))

    def __call__(self, data):
        try:
            self.callback(data)
        except:
            traceback.print_exc()

    async def handle_async(self, data):
        try:
            await self.callback(data)
        except:
            traceback.print_exc()

    async def async_callback(self, *args, **kwargs):
        return self._orig_callback(*args, **kwargs)

    async def enqueue_data_callback(self, data):
        await self._data_queue.async_q.put(data)


class Runner:

    def __init__(self, pubsub = None):
        self.pubsub = pubsub
        self.thread_id = threading.get_ident()
        self.queue = queue.Queue()
        self.consumers = SetWithLock()
        self.is_async = False

    def add_consumer(self, consumer):
        self.consumers.add(consumer)
        consumer.runner = self
        if consumer.is_async:
            self.make_async()

    def discard_consumer(self, consumer):
        self.consumers.discard(consumer)

    def publish(self, event):
        self.queue_put(event)

    def stop(self):
        self.queue_put(None)

    def get_event(self):
        return self.queue.get()

    async def get_event_async(self, wait = True):
        if wait:
            return await self.queue_async.async_q.get()
        else:
            return self.queue_async.async_q.get_nowait()

    def make_async(self):
        if not self.is_async:
            self.queue_async = janus.Queue()
            self.queue = None
            self.is_async = True

    async def close(self):
        self.queue_async.close()
        await self.queue_async.wait_closed()

    def queue_put(self, item):
        if self.is_async:
            self.queue_async.sync_q.put(item)
        else:
            self.queue.put(item)


class SetWithLock:

    def __init__(self):
        self.elems = set()
        self.lock = threading.Lock()

    def __iter__(self):
        with self.lock:
            yield from iter(self.elems)

    def __contains__(self, elem):
        return elem in self.elems

    def __bool__(self):
        return bool(self.elems)

    def add(self, elem):
        with self.lock:
            self.elems.add(elem)

    def discard(self, elem):
        with self.lock:
            self.elems.discard(elem)


pubsub = PubSub()
