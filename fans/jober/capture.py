import sys
import select
import threading
import subprocess
import contextlib
from pathlib import Path

import werkzeug.local


class Capture:
    """
    Sample usage:
    
        with Capture() as capture:
            print('foo')
            print('bar', file=sys.stderr)
        
        assert capture.out == 'foo\n'
        assert capture.err == 'bar\n'
    
    for sub process:
    
        capture = Capture()
        with capture.popen('echo foo && echo bar >&2', shell=True):
            pass
        assert capture.out == 'foo\n'
        assert capture.err == 'bar\n'
    """

    _enabled = False
    _orig_stdout = sys.stdout
    _orig_stderr = sys.stderr
    _orig___stdout__ = sys.__stdout__
    _orig___stderr__ = sys.__stderr__
    _stdout_targets = {}
    _stderr_targets = {}
    
    def __init__(self, **options):
        """
        Options:
        
            process: bool - whether for process capture, defaults to False
        
            stdout: str|None - defaults to ':memory:'
                - if None, no capture will occur
                - if ':memory:', capture into memory, accessible by `.out`
                - other str is considered as file path

            stderr: str|None - defaults to ':memory:'
                - if None, no capture will occur
                - if ':memory:', capture into memory, accessible by `.err`
                - if ':stdout:', then same as stdout
                - other str is considered as file path
        """
        self.options = options
        self.options.setdefault('stdout', ':memory:')
        self.options.setdefault('stderr', ':memory:')

        self.out_path = None
        self.err_path = None
        self.out_file = None
        self.err_file = None
        self.proc = None

        self._should_enable_disable = options.get('should_enable_disable', True)
        self._should_delete_proxy = options.get('should_delete_proxy', True)
        self._should_collect_stdout = False
        self._should_collect_stderr = False
        
        self._stdout_output = _Output()
        self._stderr_output = _Output()
    
    @property
    def out(self) -> str:
        if self.out_path:
            with self.out_path.open() as f:
                return f.read()
        else:
            return ''.join(self._stdout_output.contents)
    
    @property
    def err(self) -> str:
        if self.err_path:
            with self.err_path.open() as f:
                return f.read()
        else:
            return ''.join(self._stderr_output.contents)
    
    def popen(self, *args, **kwargs):
        """
        Create a sub process and capture its output.
        
        `args` and `kwargs` will be passed to `subprocess.Popen`, with `kwargs` updated if necessary.
        """
        stdout = self.options['stdout']
        if stdout is None:
            kwargs['stdout'] = None
        elif stdout.startswith(':'):
            if stdout == ':memory:':
                kwargs['stdout'] = subprocess.PIPE
                self._should_collect_stdout = True
        else:
            self.out_path = Path(stdout)
            kwargs['stdout'] = self.out_file = self.out_path.open('w')
        
        stderr = self.options['stderr']
        if stderr is None:
            kwargs['stderr'] = None
        elif stderr.startswith(':'):
            if stderr == ':memory:':
                kwargs['stderr'] = subprocess.PIPE
                self._should_collect_stderr = True
            elif stderr == ':stdout:':
                kwargs['stderr'] = subprocess.STDOUT
        else:
            self.err_path = Path(stderr)
            kwargs['stderr'] = self.err_file = self.err_path.open('w')

        self.proc = subprocess.Popen(*args, **kwargs)
        
        return self
    
    def __enter__(self):
        self._cm = self._enterexit()
        self._cm.__enter__()
        return self
    
    def __exit__(self, *args, **kwargs):
        self._cm.__exit__(*args, **kwargs)
    
    @contextlib.contextmanager
    def _enterexit(self):
        try:
            out_redirected = err_redirected = False

            if self.proc:
                self._collect_outputs()
                self.proc.wait()
            else:
                if self._should_enable_disable:
                    self.enable_proxy()

                if self.options['stdout'] == ':memory:':
                    _redirect_to(Capture._stdout_targets, self._stdout_output)
                    self._should_collect_stdout = True
                    out_redirected = True
                
                stderr = self.options['stderr']
                if stderr in (':memory:', ':stdout:'):
                    if stderr == ':memory:':
                        _redirect_to(Capture._stderr_targets, self._stderr_output)
                        self._should_collect_stderr = True
                    elif stderr == ':stdout:':
                        _redirect_to(Capture._stderr_targets, self._stdout_output)
                    err_redirected = True

            yield self

        finally:
            if self.out_file:
                self.out_file.close()
            if self.err_file:
                self.err_file.close()
            if out_redirected:
                _redirect_to(Capture._stdout_targets, None)
            if err_redirected:
                _redirect_to(Capture._stderr_targets, None)

            if not self.proc:
                if self._should_enable_disable:
                    self.disable_proxy()
    
    def _collect_outputs(self):
        if self._should_collect_stdout or self._should_collect_stderr:
            proc = self.proc
            fds = []
            fd_mapping = {}
            
            if self._should_collect_stdout:
                fds.append(proc.stdout.fileno())
                fd_mapping[proc.stdout.fileno()] = (proc.stdout, self._stdout_output)
            if self._should_collect_stderr:
                fds.append(proc.stderr.fileno())
                fd_mapping[proc.stderr.fileno()] = (proc.stderr, self._stderr_output)

            try:
                while fds:
                    ready_fds, _, _ = select.select(fds, [], [])
                    for fd in ready_fds:
                        stream, output = fd_mapping[fd]
                        line = stream.readline()
                        if not line:
                            fds.remove(fd)
                            continue
                        output.write(line)
            except KeyboardInterrupt:
                pass

    @staticmethod
    def enable_proxy():
        if not Capture._enabled:
            Capture._orig_stdout = sys.stdout
            Capture._orig_stderr = sys.stderr
            Capture._orig___stdout__ = sys.__stdout__
            Capture._orig___stderr__ = sys.__stderr__
            sys.stdout = werkzeug.local.LocalProxy(_make_output_getter(Capture._stdout_targets, Capture._orig_stdout))
            sys.stderr = werkzeug.local.LocalProxy(_make_output_getter(Capture._stderr_targets, Capture._orig_stderr))
            sys.__stdout__ = werkzeug.local.LocalProxy(_make_output_getter(Capture._stdout_targets, Capture._orig___stdout__))
            sys.__stderr__ = werkzeug.local.LocalProxy(_make_output_getter(Capture._stderr_targets, Capture._orig___stderr__))
            Capture._enabled = True

    @staticmethod
    def disable_proxy():
        if Capture._enabled:
            sys.stdout = Capture._orig_stdout
            sys.stderr = Capture._orig_stderr
            sys.__stdout__ = Capture._orig___stdout__
            sys.__stderr__ = Capture._orig___stderr__
            Capture._enabled = False


class _Output:
    
    def __init__(self):
        self.contents = []
    
    def write(self, content: str):
        self.contents.append(content)


def _redirect_to(targets, output):
    """
    `output` at minimal should have a `write` method taking a `str` argument
    """
    key = threading.get_ident()
    if output:
        targets[key] = output
    else:
        targets.pop(key, None)


def _make_output_getter(targets, default):
    return lambda: targets.get(threading.get_ident(), default)
