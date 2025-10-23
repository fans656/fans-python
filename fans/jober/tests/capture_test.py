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


#class TestCaptureInProcess:
#    
#    @pytest.mark.parametrize('stdout', [None, ':memory:'])
#    @pytest.mark.parametrize('stderr', [None, ':memory:'])
#    def test_inplace(self, stdout, stderr, capsys):
#        capture = Capture(stdout=stdout, stderr=stderr)
#        with capture.popen('echo foo && echo bar >&2', shell=True):
#            pass
#
#        cap = capsys.readouterr()
#
#        out_content = 'foo\n'
#
#        if stdout is None:
#            assert not capture.out
#            assert cap.out == out_content
#        elif stdout == ':memory:':
#            assert capture.out == out_content
#            assert not cap.out
#        
#        err_content = 'bar\n'
#
#        if stderr is None:
#            assert not capture.err
#            assert cap.err == err_content
#        elif stderr == ':memory:':
#            assert capture.err == err_content
#            assert not cap.err
#    
#    def test_merge(self):
#        capture = Capture(stdout=':memory:', stderr=':stdout:')
#        with capture.popen('echo foo && echo bar >&2', shell=True):
#            pass
#        
#        assert capture.out == 'foo\nbar\n'
#        assert not capture.err
