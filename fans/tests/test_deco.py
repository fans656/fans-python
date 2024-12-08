import pytest

from fans.deco import ensure_not_none


class Test_ensure_not_none:

    def test_constant_message(self):
        message = 'xxx should not be None'

        @ensure_not_none(message)
        def func():
            return 0
        assert func() == 0

        @ensure_not_none(message)
        def func():
            return None
        with pytest.raises(ValueError) as ei:
            func()
        assert str(ei.value) == message

    def test_lambda_message(self):
        @ensure_not_none(lambda name: f'not found "{name}"')
        def func(name):
            return None
        with pytest.raises(ValueError) as ei:
            func('foo')
        assert str(ei.value) == 'not found "foo"'
