from pathlib import Path

from .target import Target


class Test_command_target:

    def test_make(self, tmp_path):
        fpath = tmp_path / 'foo.txt'

        target = Target.make(['touch', f'{fpath}'])

        target()
        assert fpath.exists()
    
    def test_args(self, tmp_path):
        fpath = tmp_path / 'foo.txt'

        target = Target.make(['touch'], args=(fpath,))

        target()
        assert fpath.exists()
    
    def test_returncode(self):
        target = Target.make('exit 123', shell=True)
        assert target() == 123


class Test_python_callable_target:

    def test_make(self, mocker):
        func = mocker.Mock()

        target = Target.make(func)

        target()
        func.assert_called()

    def test_args_and_kwargs(self, mocker):
        func = mocker.Mock()

        target = Target.make(func, args=(3, 5), kwargs={'foo': 'bar'})

        target()
        func.assert_called_with(3, 5, foo='bar')


class Test_python_script_callable:

    def test_make(self, tmp_path):
        fpath = tmp_path / 'foo.txt'
        script_path = Path(__file__).absolute().parent / 'tests/samples/echo.py'

        target = Target.make(f'{script_path}:echo', args=('foo',), kwargs={'count': 3, 'file': f'{fpath}'})

        target()
        with fpath.open() as f:
            assert f.read() == 'foo\nfoo\nfoo\n'


class Test_python_module_callable:

    def test_make(self, tmp_path):
        fpath = tmp_path / 'foo.txt'

        target = Target.make(f'fans.jober.tests.samples.echo:echo', args=('foo',), kwargs={'count': 3, 'file': f'{fpath}'})

        target()
        with fpath.open() as f:
            assert f.read() == 'foo\nfoo\nfoo\n'


class Test_python_script:

    def test_make(self, tmp_path):
        fpath = tmp_path / 'foo.txt'
        script_path = Path(__file__).absolute().parent / 'tests/samples/echo.py'

        target = Target.make(f'{script_path}', args=('foo',), kwargs={'count': 3, 'file': f'{fpath}'})

        target()
        with fpath.open() as f:
            assert f.read() == 'foo\nfoo\nfoo\n'


class Test_python_module:

    def test_make(self, tmp_path):
        fpath = tmp_path / 'foo.txt'

        target = Target.make(f'fans.jober.tests.samples.echo', args=('foo',), kwargs={'count': 3, 'file': f'{fpath}'})

        target()
        with fpath.open() as f:
            assert f.read() == 'foo\nfoo\nfoo\n'


def test_xxx():
    target = Target.make('uv run -m foo "hello world"')
    target()
    #print()
