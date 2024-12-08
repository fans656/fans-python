import pytest

from fans.timing import timing


class Test_with:

    def test_simple(self, caplog):
        with timing():
            pass
        assert 'end in ' in caplog.records[0].message

    def test_name(self, caplog):
        with timing('foo'):
            pass
        assert 'beg foo' in caplog.records[0].message
        assert 'end foo in ' in caplog.records[1].message


class Test_deco:

    def test_simple(self, caplog):
        @timing
        def foo():
            pass
        foo()
        assert 'beg foo' in caplog.records[0].message
        assert 'end foo in ' in caplog.records[1].message

    def test_name(self, caplog):
        @timing('bar')
        def foo():
            pass
        foo()
        assert 'beg bar' in caplog.records[0].message
        assert 'end bar in ' in caplog.records[1].message


class Test_exception:

    def test_different_output(self, caplog):
        with pytest.raises(Exception):
            with timing():
                raise Exception()
        assert 'err' in caplog.records[0].message
