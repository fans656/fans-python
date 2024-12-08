import time

import pytest

from fans.retry import retry, _normalize_arguments


@pytest.fixture
def patch_time_sleep(mocker):
    mocker.patch('time.sleep')


def test_normalize_arguments():
    func = lambda: None
    assert _normalize_arguments() == {}
    assert _normalize_arguments(func) == {'func': func}
    assert _normalize_arguments(3) == {'times': 3}
    assert _normalize_arguments(func, 3) == {'func': func, 'times': 3}

    # kwargs is kept
    assert _normalize_arguments(wait = True) == {'wait': True}
    assert _normalize_arguments(3, wait = True) == {'times': 3, 'wait': True}

    with pytest.raises(ValueError):
        _normalize_arguments(None)

    with pytest.raises(ValueError):
        _normalize_arguments(None, None)

    with pytest.raises(ValueError):
        _normalize_arguments(None, None, None)


class Test_succeed:

    def test_s(self):
        # inline
        assert retry(make('s')) == 'succeed'

        # block
        do = make('s')
        @retry
        def result():
            return do()
        assert result == 'succeed'

    def test_xs(self):
        # inline
        assert retry(make('xs')) == 'succeed'

        # block
        do = make('xs')
        @retry
        def result():
            return do()
        assert result == 'succeed'

    def test_xxs(self):
        # inline
        assert retry(make('xxs')) == 'succeed'

        # block
        do = make('xxs')
        @retry
        def result():
            return do()
        assert result == 'succeed'


class Test_failed:

    def test_failed(self):
        # inline
        with pytest.raises(Failed):
            retry(make('xxx'), 2)

        # block
        with pytest.raises(Failed):
            do = make('xxx')
            @retry(2)
            def result():
                return do()


class Test_wait:

    def test_no_wait(self, patch_time_sleep):
        retry(make('xs'))
        assert time.sleep.call_count == 0

    def test_wait_1(self, patch_time_sleep):
        retry(make('xs'), wait = True)
        assert time.sleep.call_count == 1

    def test_wait_2(self, patch_time_sleep):
        retry(make('xxs'), wait = True)
        assert time.sleep.call_count == 2

    def test_block_wait_2(self, patch_time_sleep):
        do = make('xxs')
        @retry(wait = True)
        def result():
            return do()
        assert time.sleep.call_count == 2

    def test_wait_seconds(self, patch_time_sleep):
        retry(make('xs'), wait = 3)
        assert time.sleep.call_count == 1
        assert time.sleep.call_with(3)


class Test_generator:

    def test_success(self):
        do = make([1])
        @retry
        def result():
            if not do():
                yield
            return 'succeed'
        assert result == 'succeed'

    def test_retries(self):
        do = make([0, 0, 1])
        @retry
        def result():
            if not do():
                yield
            return 'succeed'
        assert result == 'succeed'

    def test_raise_upon_execution_limit(self):
        do = make('xxxx')
        with pytest.raises(Failed):
            @retry(3)
            def result():
                if not do():
                    yield
                return 'succeed'


class Test_log:

    def test_log_fail(self, caplog):
        prefix = 'retry failed upon reaching limit'

        # inline
        with pytest.raises(Failed):
            retry(make('xxxx'), 3, log = 'fail')
        assert caplog.records[0].message.startswith(prefix)

        # generator
        do = make('xxxx')
        with pytest.raises(Failed):
            @retry(3, log = 'fail')
            def result():
                if not do():
                    yield
                return 'succeed'
        assert caplog.records[1].message.startswith(prefix)

    def test_log_wait(self, caplog, patch_time_sleep):
        message = 'wait 1s before 1/1 retry'

        # inline
        retry(make('xs'), 2, wait = True, log = 'wait')
        assert caplog.records[0].message == message

        # generator
        do = make('xs')
        @retry(2, wait = True, log = 'wait')
        def result():
            if not do():
                yield
            return 'succeed'
        assert caplog.records[1].message == message

    def test_log_trace(self, caplog):
        # inline
        with pytest.raises(Failed):
            retry(make('xx'), 2, log = 'trace')
        assert 'Traceback' in caplog.records[0].message

        # generator
        do = make('xx')
        with pytest.raises(Failed):
            @retry(2, log = 'trace')
            def result():
                if not do():
                    yield
                return 'succeed'
        assert 'Traceback' in caplog.records[1].message


class Failed(Exception):

    pass


def make(fmt):
    tokens = list(reversed(fmt))
    def func():
        if tokens:
            token = tokens.pop()
            if isinstance(token, str):
                match token:
                    case 'x':
                        raise Failed('failed')
                    case 'f':
                        return False
                    case 't':
                        return True
                    case 's':
                        return 'succeed'
                    case _:
                        return None
            elif isinstance(token, int):
                return token
            else:
                raise ValueError(f'invalid token: {token} in {tokens}')
        else:
            raise ValueError('no spec')
    return func
