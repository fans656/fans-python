import sys
import time
from pathlib import Path

import pytest

from fans.jober.capture import Capture


class TestCaptureInplace:
    
    @pytest.mark.parametrize('stdout', [None, ':memory:'])
    @pytest.mark.parametrize('stderr', [None, ':memory:'])
    def test_inplace(self, stdout, stderr, capsys):
        with Capture(stdout=stdout, stderr=stderr) as capture:
            print('foo')
            print('bar', file=sys.stderr)

        cap = capsys.readouterr()

        out_content = 'foo\n'

        if stdout is None:
            assert not capture.out
            assert cap.out == out_content
        elif stdout == ':memory:':
            assert capture.out == out_content
            assert not cap.out
        
        err_content = 'bar\n'

        if stderr is None:
            assert not capture.err
            assert cap.err == err_content
        elif stderr == ':memory:':
            assert capture.err == err_content
            assert not cap.err
    
    def test_merge(self):
        with Capture(stdout=':memory:', stderr=':stdout:') as capture:
            print('foo')
            print('bar', file=sys.stderr)
        
        assert capture.out == 'foo\nbar\n'
        assert not capture.err


class TestCaptureInProcess:
    
    @pytest.mark.parametrize('stdout', [None, ':memory:', 'file'], ids=lambda v: f'out-{v}')
    @pytest.mark.parametrize('stderr', [None, ':memory:', 'file'], ids=lambda v: f'err-{v}')
    def test_inplace(self, stdout, stderr, capfd, tmp_path):
        capture_kwargs = {'stdout': stdout, 'stderr': stderr}
        
        if stdout == 'file':
            stdout_fpath = tmp_path / 'stdout.log'
            capture_kwargs['stdout'] = str(stdout_fpath)
        if stderr == 'file':
            stderr_fpath = tmp_path / 'stderr.log'
            capture_kwargs['stderr'] = str(stderr_fpath)

        with Capture(**capture_kwargs).popen(
            'echo foo && echo bar >&2',
            shell=True,
            text=True,
        ) as capture:
            pass

        cap = capfd.readouterr()

        out_content = 'foo\n'

        if stdout is None:
            assert not capture.out
            assert cap.out == out_content
        elif stdout == ':memory:':
            assert capture.out == out_content
            assert not cap.out
        elif stdout == 'file':
            assert capture.out == out_content
            with stdout_fpath.open() as f:
                assert f.read() == out_content
            assert not cap.out
        
        err_content = 'bar\n'

        if stderr is None:
            assert not capture.err
            assert cap.err == err_content
        elif stderr == ':memory:':
            assert capture.err == err_content
            assert not cap.err
        elif stderr == 'file':
            assert capture.err == err_content
            with stderr_fpath.open() as f:
                assert f.read() == err_content
            assert not cap.err
    
    def test_merge(self):
        capture = Capture(stdout=':memory:', stderr=':stdout:')
        with capture.popen('echo foo && echo bar >&2', shell=True, text=True):
            pass
        
        assert capture.out == 'foo\nbar\n'
        assert not capture.err
    
    def test_merge_file(self, tmp_path):
        out_fpath = tmp_path / 'out.log'
        capture = Capture(stdout=str(out_fpath), stderr=':stdout:')
        with capture.popen('echo foo && echo bar >&2', shell=True, text=True):
            pass
        
        assert capture.out == 'foo\nbar\n'
        with out_fpath.open() as f:
            assert f.read() == 'foo\nbar\n'
        assert not capture.err
