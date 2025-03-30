import threading

from .sync import process_sync
from .context import Context


def test_action_called(mocker):
    client_func = mocker.Mock()
    server_func = mocker.Mock()

    def sync(ctx):
        @ctx.client
        def _():
            client_func()
        yield _

        @ctx.server
        def _():
            server_func()
        yield _

    process_sync(sync, Context.Client())
    assert client_func.call_count == 1
    assert server_func.call_count == 0

    process_sync(sync, Context.Server())
    assert client_func.call_count == 1
    assert server_func.call_count == 1


def test_communicate(mocker):
    verify = mocker.Mock()

    def sync(ctx):
        @ctx.client
        def _():
            return 'foo'
        yield _

        @ctx.server
        def _():
            return ctx.data.upper()
        yield _

        @ctx.client
        def _():
            verify(ctx.data)
        yield _

    client_ctx = Context.Client()
    server_ctx = Context.Server()

    client_ctx.send = server_ctx.recv
    server_ctx.send = client_ctx.recv

    client_thread = threading.Thread(target=process_sync, args=(sync, client_ctx))
    server_thread = threading.Thread(target=process_sync, args=(sync, server_ctx))

    client_thread.start()
    server_thread.start()
    client_thread.join()
    server_thread.join()

    verify.assert_called_with('FOO')
