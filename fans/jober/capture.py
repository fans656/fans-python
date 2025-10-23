import sys
import select
import threading
import subprocess
from pathlib import Path

import werkzeug.local


class Capture:

    _orig_stdout = sys.stdout
    _orig_stderr = sys.stderr
    _orig___stdout__ = sys.__stdout__
    _orig___stderr__ = sys.__stderr__
    
    def __init__(self, **options):
        """
        Options:
        
            process: bool - whether for process capture, defaults to False
        
            stdout: str|None - defaults to ':memory:'
                - if None, no capture is done
                - if ':memory:', capture into memory, accessible by `.stdout`, `.stdout_lines`
                - other str considered as file path

            stderr: str|None - defaults to ':memory:'
                - if ':stdout:', then same as stdout
        """
        self.options = options
        self.options.setdefault('stdout', ':memory:')
        self.options.setdefault('stderr', ':memory:')
        
        self.popen_kwargs = {}

        self.stdout_file = None
        self.stderr_file = None
        
        self._for_process = False
        self._should_enable_disable = options.get('should_enable_disable', True)
        self._should_delete_proxy = options.get('should_delete_proxy', True)
        self._should_collect_stdout = False
        self._should_collect_stderr = False
        self._stdout_redirected = False
        self._stderr_redirected = False
        
        self._stdout_output = _Output()
        self._stderr_output = _Output()
    
    @property
    def out(self) -> str:
        return ''.join(self._stdout_output.contents)
    
    @property
    def err(self) -> str:
        return ''.join(self._stderr_output.contents)
    
    def popen(self, *args, **kwargs):
        self._for_process = True

        stdout = self.options['stdout']
        if stdout is None:
            self.popen_kwargs['stdout'] = None
        elif stdout.startswith(':'):
            if stdout == ':memory:':
                self.popen_kwargs['stdout'] = subprocess.PIPE
                self._should_collect_stdout = True
        else:
            self.stdout_file = Path(stdout).open('w')
            self.popen_kwargs['stdout'] = self.stdout_file
        
        stderr = self.options['stderr']
        if stderr is None:
            self.popen_kwargs['stderr'] = None
        elif stderr.startswith(':'):
            if stderr == ':memory:':
                self.popen_kwargs['stderr'] = subprocess.PIPE
                self._should_collect_stderr = True
            elif stderr == ':stdout:':
                self.popen_kwargs['stderr'] = subprocess.STDOUT
        else:
            self.stderr_file = Path(stderr).open('w')
            self.popen_kwargs['stderr'] = self.stderr_file

        kwargs.update(self.popen_kwargs)

        proc = subprocess.Popen(*args, **kwargs)

        if self._should_collect_stdout or self._should_collect_stderr:
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

        proc.wait()
        
        return proc
    
    def __enter__(self):
        if not self._for_process:
            if self._should_enable_disable:
                enable_proxy()

            if self.options['stdout'] == ':memory:':
                self._should_collect_stdout = True
                _redirect_to(_stdout_targets, self._stdout_output)
                self._stdout_redirected = True
            
            stderr = self.options['stderr']
            if stderr in (':memory:', ':stdout:'):
                if stderr == ':memory:':
                    _redirect_to(_stderr_targets, self._stderr_output)
                elif stderr == ':stdout:':
                    _redirect_to(_stderr_targets, self._stdout_output)
                self._stderr_redirected = True
        
        return self
    
    def __exit__(self, *_, **__):
        if self.stdout_file:
            self.stdout_file.close()
        if self.stderr_file:
            self.stderr_file.close()
        if self._stdout_redirected:
            _redirect_to(_stdout_targets, None)
        if self._stderr_redirected:
            _redirect_to(_stderr_targets, None)

        if not self._for_process:
            if self._should_enable_disable:
                disable_proxy()


class _Output:
    
    def __init__(self):
        self.contents = []
    
    def write(self, content: str):
        self.contents.append(content)


_enabled = False
_stdout_targets = {}
_stderr_targets = {}


def enable_proxy():
    global _enabled
    if not _enabled:
        Capture._orig_stdout = sys.stdout
        Capture._orig_stderr = sys.stderr
        Capture._orig___stdout__ = sys.__stdout__
        Capture._orig___stderr__ = sys.__stderr__
        sys.stdout = werkzeug.local.LocalProxy(_make_output_getter(_stdout_targets, Capture._orig_stdout))
        sys.stderr = werkzeug.local.LocalProxy(_make_output_getter(_stderr_targets, Capture._orig_stderr))
        sys.__stdout__ = werkzeug.local.LocalProxy(_make_output_getter(_stdout_targets, Capture._orig___stdout__))
        sys.__stderr__ = werkzeug.local.LocalProxy(_make_output_getter(_stderr_targets, Capture._orig___stderr__))
        _enabled = True


def disable_proxy():
    global _enabled
    if _enabled:
        sys.stdout = Capture._orig_stdout
        sys.stderr = Capture._orig_stderr
        sys.__stdout__ = Capture._orig___stdout__
        sys.__stderr__ = Capture._orig___stderr__
        _enabled = False


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
