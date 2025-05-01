import dill
import threading

import pytest
from starlette.testclient import TestClient

from .talks import run_client
from .app import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def send(client):
    def _request(req):
        res = client.post('/api/fans/talks', json=req)
        assert res.status_code == 200
        return res.json()
    return _request


def test_simple_req_and_res(client, send, mocker):
    """
    client          server
    |                   |
    |------------------>| values: [3, 5]
    |<------------------| result: 8
    |                   |
    """
    mock = mocker.Mock()

    def my_talks(ctx):
        @ctx.client
        def _():
            return {'values': [3, 5]}  # <--- client request

        @ctx.server
        def _(req):
            return {'result': sum(req['values'])}  # <--- server respond
        
        @ctx.client
        def _(res):
            mock(res)  # <--- client got response
    
    run_client(my_talks, request=send)

    mock.assert_called_with({'result': 8})


def test_loop(client, send):
    """
    client          server
    |                   |
    |------------------>| value: 0
    |<------------------| value: 1
    |                   |
    |------------------>| value: 1
    |<------------------| value: 2
    |                   |
    |------------------>| value: 2
    |<------------------| value: 3
    |                   |
    """
    res_values = []

    def my_talks(ctx):
        with ctx.loop:
            @ctx.client
            def _():
                return {'value': 0}  # <--- initial request

            @ctx.server
            def _(req):
                return {'value': req['value'] + 1}  # <--- server respond

            @ctx.client
            def _(res):
                if res['value'] < 3:
                    return {'value': res['value']}  # <--- continue
    
    run_client(my_talks, request=send)

    assert res_values == [1, 2, 3]


#def test_loop_yield_value(client, send):
#    """
#    client          server
#    |                   |
#    |------------------>| value: 0
#    |<------------------| value: 1
#    |                   |
#    |------------------>| value: 1
#    |<------------------| value: 2
#    |                   |
#    |------------------>| value: 2
#    |<------------------| value: 3
#    |                   |
#    """
#    res_values = []
#
#    def my_talks(ctx):
#        with ctx.loop:
#            @ctx.client
#            def _(res):
#                res = yield {'value': 0}
#                if res['value'] < 3:
#                    return {'value': res['value']}
#
#            @ctx.server
#            def _(req):
#                return {'value': req['value'] + 1}  # <--- server respond
#    
#    run_client(my_talks, request=send)
#
#    assert res_values == [1, 2, 3]


#def test_loop_yield_lambda(client, send):
#    """
#    client          server
#    |                   |
#    |------------------>| value: 0
#    |<------------------| value: 1
#    |                   |
#    |------------------>| value: 1
#    |<------------------| value: 2
#    |                   |
#    |------------------>| value: 2
#    |<------------------| value: 3
#    |                   |
#    """
#    res_values = []
#
#    def my_talks(ctx):
#        prepare_heavy_request = lambda: {'value': 0}
#
#        with ctx.loop:
#            @ctx.client
#            def _(res):
#                res = yield lambda: prepare_heavy_request()
#                if res['value'] < 3:
#                    return {'value': res['value']}
#
#            @ctx.server
#            def _(req):
#                return {'value': req['value'] + 1}  # <--- server respond
#    
#    run_client(my_talks, request=send)
#
#    assert res_values == [1, 2, 3]


#def test_default(client):
#
#    def my_talks(ctx):
#        @ctx.client
#        def _():
#            return {'foo': 3}
#
#        @ctx.server('inc')
#        def _():
#            return {'bar': 5}
#        
#        @ctx.client
#        def _(res):
#            if res['value'] > 0:
#                return {'foo': 10}, 'inc'
#            else:
#                return {'next': ...}
#        
#        with ctx.loop:
#            @ctx.client
#            def _(res):
#                res = yield req
#                return res['value'] > 10
#            
#            @ctx.server
#            def _():
#                pass
#        
#        with ctx.parallel:
#            @ctx.client
#            def _():
#                pass
#            @ctx.server
#            def _():
#                pass
#        
#        with ctx.parallel:
#            @ctx.client
#            def _():
#                pass
#            @ctx.server
#            def _():
#                pass
#
#    def my_talks(ctx):
#        @ctx.client
#        def _():
#            pass
#    
#    def request(req):
#        return client.post('/api/fans/talks', json=req).json()
#    
#    run_client(my_talks, request=request)
