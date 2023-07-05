from fans.fn import omit


class Test_omit:

    def test_all(self):
        assert omit({'a': 3, 'b': 4}, ['a']) == {'b': 4}
