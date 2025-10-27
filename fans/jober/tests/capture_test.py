import sys
from pathlib import Path

import pytest

from fans.jober.capture import Capture


@pytest.mark.parametrize('stderr', [None, ':memory:', 'file', ':stdout:'], ids=lambda v: f'err-{v}')
@pytest.mark.parametrize('stdout', [None, ':memory:', 'file'], ids=lambda v: f'out-{v}')
@pytest.mark.parametrize('mode', ['inplace', 'process'], ids=lambda v: f'mode-{v}')
def test_capture(mode, stdout, stderr, capfd, tmp_path):
    capture_kwargs = {'stdout': stdout, 'stderr': stderr}
    
    if stdout == 'file':
        stdout_fpath = tmp_path / 'stdout.log'
        capture_kwargs['stdout'] = str(stdout_fpath)
    if stderr == 'file':
        stderr_fpath = tmp_path / 'stderr.log'
        capture_kwargs['stderr'] = str(stderr_fpath)

    if mode == 'inplace':
        with Capture(**capture_kwargs) as capture:
            print('foo')
            print('bar', file=sys.stderr)
    elif mode == 'process':
        with Capture(**capture_kwargs).popen(
            'echo foo && echo bar >&2',
            shell=True,
            text=True,
        ) as capture:
            pass
    
    if stderr == ':stdout:':
        out_content = 'foo\nbar\n'
        if stdout is not None:
            assert capture.out == out_content
            assert not capture.err
            if stdout == 'file':
                with stdout_fpath.open() as f:
                    assert f.read() == out_content
    else:
        out_content = 'foo\n'
        err_content = 'bar\n'
        cap = capfd.readouterr()

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
