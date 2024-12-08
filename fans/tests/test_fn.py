from fans.fn import omit, chunks


class Test_omit:

    def test_all(self):
        assert omit({'a': 3, 'b': 4}, ['a']) == {'b': 4}


class Test_chunk:

    def test_normal(self):
        assert list(chunks(range(5), 2)) == [
            [0, 1], [2, 3], [4],
        ]

    def test_count(self):
        assert list(chunks(range(5), 2, count = True)) == [
            ((0, 2), [0, 1]),
            ((2, 4), [2, 3]),
            ((4, 5), [4]),
        ]
