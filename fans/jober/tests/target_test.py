import time
import threading
from pathlib import Path

import pytest
from fans.jober.target import (
    Target,
    _get_impl_cls,
    CommandTarget,
    PythonCallableTarget,
    PythonScriptCallableTarget,
    PythonModuleCallableTarget,
    PythonScriptTarget,
    PythonModuleTarget,
)
from fans.jober.tests.samples.echo import echo


def test_types():
    assert _get_impl_cls('date') is CommandTarget
    assert _get_impl_cls('ls -lh') is CommandTarget
    assert _get_impl_cls(['ls', '-lh']) is CommandTarget
    assert _get_impl_cls('./main') is CommandTarget
    assert _get_impl_cls('t.py', shell=True) is CommandTarget

    assert _get_impl_cls(lambda: None) is PythonCallableTarget
    assert _get_impl_cls(type('', (), {'__call__': lambda: None})) is PythonCallableTarget

    assert _get_impl_cls('crawl.py:main') is PythonScriptCallableTarget

    assert _get_impl_cls('crawl.prices:main') is PythonModuleCallableTarget

    assert _get_impl_cls('crawl.py') is PythonScriptTarget

    assert _get_impl_cls('crawl.prices') is PythonModuleTarget


class Test_command_target:

    def test_simple(self, tmp_path):
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


class Test_python_script:

    def test_make(self, tmp_path):
        fpath = tmp_path / 'foo.txt'
        script_path = Path(__file__).absolute().parent / 'samples/echo.py'

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

    def test_execute_in_process(self, tmp_path):
        out_fpath = tmp_path / 'out.txt'

        target = Target.make(
            echo,
            args=['foo'],
            kwargs={'file': str(out_fpath)},
            process=True,
        )
        target()
        
        with out_fpath.open() as f:
            assert f.read() == 'foo\n'


class Test_python_script_callable:

    def test_make(self, tmp_path):
        fpath = tmp_path / 'foo.txt'
        script_path = Path(__file__).absolute().parent / 'samples/echo.py'

        target = Target.make(f'{script_path}:echo', args=('foo',), kwargs={'count': 3, 'file': f'{fpath}'})
        target()

        with fpath.open() as f:
            assert f.read() == 'foo\nfoo\nfoo\n'

    def test_execute_in_process(self, tmp_path):
        out_fpath = tmp_path / 'out.txt'
        script_path = Path(__file__).absolute().parent / 'samples/echo.py'

        target = Target.make(
            f"{script_path}:echo",
            args=['foo'],
            kwargs={'file': str(out_fpath)},
            process=True,
        )
        target()
        
        with out_fpath.open() as f:
            assert f.read() == 'foo\n'


class Test_python_module_callable:

    def test_make(self, tmp_path):
        fpath = tmp_path / 'foo.txt'

        target = Target.make(f'fans.jober.tests.samples.echo:echo', args=('foo',), kwargs={'count': 3, 'file': f'{fpath}'})
        target()

        with fpath.open() as f:
            assert f.read() == 'foo\nfoo\nfoo\n'

    def test_execute_in_process(self, tmp_path):
        out_fpath = tmp_path / 'out.txt'

        target = Target.make(
            'fans.jober.tests.samples.echo:echo',
            args=['foo'],
            kwargs={'file': str(out_fpath)},
            process=True,
        )
        target()
        
        with out_fpath.open() as f:
            assert f.read() == 'foo\n'


class Test_unbuffered:
    
    @pytest.fixture
    def verify(self, tmp_path):

        def _verify(source):
            stop_fpath = tmp_path / 'stop'

            target = Target.make(source, kwargs={
                'interval': 0.01,
                'stop-file': str(stop_fpath),
            })
            print(target)

            thread = threading.Thread(target=target)
            thread.start()

            for _ in range(5):
                time.sleep(0.1)
                out = target.capture.out_str.strip()
                n_lines = len(out.split('\n'))
                if n_lines > 1:
                    break
            assert n_lines > 1

            stop_fpath.touch()
            thread.join()

        return _verify
    
    def test_python_script(self, verify, samples_dir):
        verify(str(samples_dir / 'counting.py'))
    
    def test_python_module(self, verify):
        verify('fans.jober.tests.samples.counting')
