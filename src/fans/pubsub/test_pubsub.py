import asyncio
import itertools
import threading

import pytest

from .pubsub import PubSub, nested_topics


@pytest.fixture
def pubsub():
    yield PubSub()


@pytest.fixture
def threaded():
    return Threaded()


@pytest.fixture
def noerr(capsys):
    try:
        yield
    finally:
        assert not capsys.readouterr().err


class Test_single_thread:

    def test_pub_without_subs_do_nothing(self, pubsub, noerr):
        """
        One can just publish, if no subscribers, then nothing happened.
        """
        pubsub.publish('foo', 3)

    def test_pub_with_sub_will_run_the_sub(self, pubsub, mocker):
        """
        In the same thread, subscribers will be invoked immediately upon publish.
        """
        callback = mocker.Mock()

        pubsub.subscribe('', callback)
        pubsub.publish('',  42)

        callback.assert_called_once_with('', 42)

    def test_multi_subs_on_same_topic_with_same_callback(self, pubsub, mocker):
        """
        Use same callback to subscribe same topic multiple times is same as subscribe only once.
        """
        callback = mocker.Mock()

        pubsub.subscribe('', callback)
        pubsub.subscribe('', callback)
        pubsub.publish('', 42)

        callback.assert_called_once_with('', 42)

    def test_exception_is_ignored(self, pubsub, mocker):
        callback1 = mocker.Mock(side_effect = Exception('oops'))
        callback2 = mocker.Mock()

        pubsub.subscribe('', callback1)
        pubsub.subscribe('', callback2)
        pubsub.publish('', 42)

        callback1.assert_called_once_with('', 42)
        callback2.assert_called_once_with('', 42)


class Test_unsubscribe:

    def test_unsub_by_callback(self, pubsub, mocker):
        callback = mocker.Mock()

        pubsub.subscribe('', callback)
        pubsub.publish('', 42)
        callback.assert_called_once()

        pubsub.unsubscribe(callback)
        pubsub.publish('', 42)
        callback.assert_called_once()

    def test_unsub_by_token(self, pubsub, mocker):
        callback = mocker.Mock()

        token = pubsub.subscribe('', callback)
        pubsub.publish('', 42)
        callback.assert_called_once()

        pubsub.unsubscribe(token)
        pubsub.publish('', 42)
        callback.assert_called_once()

    def test_unsub_should_not_affect_other_callbacks(self, pubsub, mocker):
        callback1 = mocker.Mock()
        callback2 = mocker.Mock()

        pubsub.subscribe('', callback1)
        pubsub.subscribe('', callback2)
        pubsub.publish('', 42)

        pubsub.unsubscribe(callback1)
        pubsub.publish('', 42)

        assert callback1.call_count == 1
        assert callback2.call_count == 2


class Test_multi_threads:

    def test_pub_will_trigger_sub(self, pubsub, mocker, threaded):
        """
        Publish will trigger subscriber even it's subscribed in another thread.

        Note that subscriber will be run in publisher thread.
        """
        callback = mocker.Mock()
        _subscribed = threading.Event()
        with threaded:
            @threaded
            def _():
                pubsub.subscribe('', callback)
                _subscribed.set()
            @threaded
            def i():
                _subscribed.wait()
                pubsub.publish('', 42)
        callback.assert_called_once_with('', 42)


class Test_events:

    def test_sub_is_not_in_effect_immediately_if_no_callback(self, pubsub, noerr):
        pubsub.subscribe()
        pubsub.publish('', 42)

    def test_events(self, pubsub, threaded):
        _subscribed = threading.Event()

        with threaded:
            @threaded
            def _():
                try:
                    events = pubsub.subscribe().events
                    _subscribed.set()
                    assert list(itertools.islice(events, 3)) == [
                        ('', 1),
                        ('', 2),
                        ('', 3),
                    ]
                except:
                    _subscribed.set()
                    raise

            @threaded
            def _():
                _subscribed.wait()
                pubsub.publish('', 1)
                pubsub.publish('', 2)
                pubsub.publish('', 3)

    def test_context_manager(self, pubsub):
        sub = pubsub.subscribe()
        with sub:
            pass
        assert not pubsub.subscribed(sub)


class Test_async_events:

    async def test_anext(self, pubsub, threaded):
        """
        Events can be retrieved by calling `anext`.

            async with pubsub.subscribe().async_events as events:
                event = await anext(events)
        """
        async with pubsub.subscribe().async_events as events:

            with threaded:
                @threaded
                def _():
                    pubsub.publish('', 1)
                    pubsub.publish('', 2)

            assert await anext(events) == ('', 1)
            assert await anext(events) == ('', 2)

    async def test_async_for(self, pubsub, threaded):
        """
        Events can be retrieved by using `async for`.

            with pubsub.subscribe().async_events as events:
                async for event in events:
                    print(event)
        """
        async with pubsub.subscribe().async_events as events:

            with threaded:
                @threaded
                def _():
                    pubsub.publish('', 1)
                    pubsub.publish('', 2)

            collected = []
            async for event in events:
                collected.append(event)
                if len(collected) == 2:
                    break

            assert collected == [('', 1), ('', 2)]

    async def test_get(self, pubsub, threaded):
        """
        Events can be retrieved by using `.get`.

            with pubsub.subscribe().async_events as events:
                print(events.get())
        """
        async with pubsub.subscribe().async_events as events:

            with threaded:
                @threaded
                def _():
                    pubsub.publish('', 42)

            assert await events.get() == ('', 42)

    async def test_get_timeout(self, pubsub, threaded):
        """
        Events can be retrieved by using `.get` with timeout.

            with pubsub.subscribe().async_events as events:
                try:
                    print(events.get(0.02))
                except TimeoutError:
                    pass
        """
        async with pubsub.subscribe().async_events as events:
            with pytest.raises(asyncio.exceptions.TimeoutError):
                await events.get(0.01)


class Test_topic_hierachy:

    def test_root_sub_can_receive_all(self, pubsub, mocker):
        root_callback = mocker.Mock()
        foo_callback = mocker.Mock()
        foo_bar_callback = mocker.Mock()

        pubsub.subscribe('', root_callback)
        pubsub.subscribe('foo', foo_callback)
        pubsub.subscribe('foo.bar', foo_bar_callback)

        pubsub.publish('foo.bar', 42)
        foo_bar_callback.assert_called_once_with('foo.bar', 42)
        foo_callback.assert_called_once_with('foo.bar', 42)
        root_callback.assert_called_once_with('foo.bar', 42)

    def test_can_use_asterisk_for_root_sub(self, pubsub, mocker):
        callback = mocker.Mock()
        pubsub.subscribe('*', callback)
        pubsub.publish('foo.bar', 42)
        callback.assert_called_once_with('foo.bar', 42)

    def test_can_skip_topic_for_root_sub(self, pubsub, mocker):
        callback = mocker.Mock()
        pubsub.subscribe(callback)
        pubsub.publish('foo.bar', 42)
        callback.assert_called_once_with('foo.bar', 42)


def test_nested_topics():
    assert list(nested_topics('')) == ['']
    assert list(nested_topics('foo')) == ['foo', '']
    assert list(nested_topics('foo.bar')) == ['foo.bar', 'foo', '']


class Threaded:

    def __init__(self):
        self.funcs = []

    def __call__(self, func):
        self.funcs.append(func)

    def __enter__(self):
        pass

    def __exit__(self, *_):
        threads = [threading.Thread(target = func) for func in self.funcs]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
