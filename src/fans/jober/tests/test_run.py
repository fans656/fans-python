import time

import pytest

from fans.poll import until, threaded
from fans.jober.run import Run


def test_constructor(tmp_path):
    run = Run({
        'type': 'command',
        'cmd': 'ls',
    }, tmp_path)
    assert run.meta_path.exists()
    assert run.out_file

    with run.meta_path.open('w') as f:
        f.write('foo')
    run = Run({
        'type': 'command',
        'cmd': 'ls',
    }, tmp_path)
    with run.meta_path.open() as f:
        assert f.read() == 'foo'
    assert run.out_file is None


def test_status_done(tmp_path):
    run = Run({
        'type': 'command',
        'cmd': 'while [ ! -f ./go ]; do sleep 0.1; done',
        'cwd': str(tmp_path),
    }, tmp_path)
    assert run.status == 'ready'

    threaded(run)
    until(lambda: run.meta_path.exists())
    assert run.status == 'running'

    (tmp_path / 'go').touch()
    until(lambda: run.status != 'running')
    assert run.status == 'done'


def test_status_error(tmp_path):
    run = Run({
        'type': 'command',
        'cmd': 'while true; do sleep 1; done',
    }, tmp_path)
    assert run.status == 'ready'

    threaded(run)
    until(lambda: run.terminate())
    until(lambda: run.status != 'running')
    assert run.status == 'error'


def test_command_run(tmp_path):
    run = Run({
        'type': 'command',
        'cmd': 'echo "hello world"',
    }, tmp_path)
    run()
    assert run.output == 'hello world\n'


def test_script_run(tmp_path):
    fname = 't.py'
    fpath = tmp_path / fname
    with fpath.open('w') as f:
        f.write('print("hello world")')

    run = Run({
        'type': 'script',
        'script': fname,
        'cwd': str(tmp_path),
    }, tmp_path)
    run()
    assert run.output == 'hello world\n'


def test_script_run_with_args(tmp_path):
    fname = 't.py'
    fpath = tmp_path / fname
    with fpath.open('w') as f:
        f.write('import sys; print(sys.argv[1])')

    run = Run({
        'type': 'script',
        'script': fname,
        'args': ['foo'],
        'cwd': str(tmp_path),
    }, tmp_path)
    run()
    assert run.output == 'foo\n'


def test_module_run(tmp_path):
    fname = 't.py'
    fpath = tmp_path / fname
    with fpath.open('w') as f:
        f.write('print("hello world")')

    run = Run({
        'type': 'module',
        'module': fname.split('.')[0],
        'cwd': str(tmp_path),
    }, tmp_path)
    run()
    assert run.output == 'hello world\n'


def test_module_run_with_args(tmp_path):
    fname = 't.py'
    fpath = tmp_path / fname
    with fpath.open('w') as f:
        f.write('import sys; print(sys.argv[1])')

    run = Run({
        'type': 'module',
        'module': fname.split('.')[0],
        'args': ['foo'],
        'cwd': str(tmp_path),
    }, tmp_path)
    run()
    assert run.output == 'foo\n'


def test_on_event(mocker, tmp_path):
    valid_run_spec = {
        'type': 'command',
        'cmd': 'ls',
    }
    invalid_run_spec = {
        'type': 'command',
        'cmd': 'the-quick-brown-fox-jumps-over-the-lazy-dog',
    }

    # normal case: on_event will be called in valid run
    on_event = mocker.Mock()
    run = Run(valid_run_spec, run_dir = tmp_path, on_event = on_event)
    run()
    on_event.assert_called()

    # normal case: on_event will be called in invalid run
    on_event = mocker.Mock()
    run = Run(invalid_run_spec, run_dir = tmp_path, on_event = on_event)
    run()
    on_event.assert_called()

    # edge case: if on_event is not given, run can still work
    run = Run(valid_run_spec, run_dir = tmp_path)
    run()


def test_iter_output(tmp_path):
    cwd = tmp_path / 'cwd'
    cwd.mkdir()
    run = Run({
        'type': 'script',
        'script': '../' + saved_script(tmp_path, output_script_content).name,
        'cwd': str(cwd),
    }, tmp_path)
    threaded(run)
    out = run.iter_output()

    (cwd / '1').touch()
    assert next(out) == '1'

    (cwd / '2').touch()
    assert next(out) == '2'

    (cwd / '3').touch()
    assert next(out) == '3'

    with pytest.raises(StopIteration):
        next(out)


@pytest.mark.asyncio
async def test_iter_output_async(tmp_path):
    cwd = tmp_path / 'cwd'
    cwd.mkdir()
    run = Run({
        'type': 'script',
        'script': '../' + saved_script(tmp_path, output_script_content).name,
        'cwd': str(cwd),
    }, tmp_path)
    threaded(run)
    out = run.iter_output_async()
    (cwd / '1').touch()
    count = 1
    async for line in out:
        assert line == f'{count}'
        count += 1
        (cwd / f'{count}').touch()


def test_kill_run(tmp_path):
    run = Run({
        'type': 'command',
        'cmd': 'while true; do sleep 1; done',
    }, tmp_path)
    threaded(run)
    until(run.kill)
    until(lambda: run.status != 'running', timeout = 1)
    assert run.status == 'error'


def test_terminate_run(tmp_path):
    run = Run({
        'type': 'command',
        'cmd': 'while true; do sleep 1; done',
    }, tmp_path)
    threaded(run)
    until(run.terminate)
    until(lambda: run.status != 'running', timeout = 1)
    assert run.status == 'error'


def saved_script(path, content, fname = 't.py'):
    fpath = path / fname
    with fpath.open('w') as f:
        f.write(content)
    return fpath


output_script_content = '''
import sys
import time
from pathlib import Path

known = set()
limit = int(sys.argv[1]) if len(sys.argv) > 1 else 3
while len(known) < limit:
    paths = list(Path().iterdir())
    for path in paths:
        if path.name not in known:
            print(path.name)
            known.add(path.name)
    time.sleep(0.01)
'''
