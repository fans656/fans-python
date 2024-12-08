from fans.vectorized import Vectorized


class People:

    def __init__(self, name):
        self.name = name

    def say(self):
        return f'i am {self.name}'


peoples = list(map(People, ['alice', 'bob']))


class Test_Vectorized:

    def test_get_attr(self):
        a = Vectorized(peoples)
        assert list(a.name) == ['alice', 'bob']

    def test_call_method(self):
        a = Vectorized(peoples)
        assert list(a.say()) == ['i am alice', 'i am bob']

    def test_bool(self):
        assert not Vectorized([])
        assert Vectorized([1, 2, 3])

    def test_len(self):
        assert len(Vectorized([])) == 0
        assert len(Vectorized([1, 2, 3])) == 3

    def test_call_immediately(self, mocker):
        for p in peoples:
            p.die = mocker.Mock()
        a = Vectorized(peoples)
        a.die()
        for p in a:
            p.die.assert_called()
