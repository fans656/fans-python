import sys
import select
import asyncio
import threading
import subprocess
import contextlib
from pathlib import Path

import aiofiles
from werkzeug.local import LocalProxy


class Capture:
    """
    Inplace capture:
    
        with Capture() as capture:
            print('foo')
            print('bar', file=sys.stderr)
        
        assert capture.out_str == 'foo\n'
        assert capture.err_str == 'bar\n'
    
    sub process capture:
    
        with Capture().popen('echo foo && echo bar >&2', shell=True) as capture:
            pass
        assert capture.out_str == 'foo\n'
        assert capture.err_str == 'bar\n'
    
    merge stderr into stdout:
    
        with Capture(stderr=':stdout:') as capture:
            print('foo')
            print('bar', file=sys.stderr)
        
        assert capture.out_str == 'foo\nbar\n'
    
   into file:
    
        with Capture(stdout='/tmp/out.log') as capture:
            print('foo')
        
        assert capture.out_path.open().read() == 'foo\n'
    """

    _enabled = False
    _sys_stdout = sys.stdout
    _sys_stderr = sys.stderr
    _sys__stdout__ = sys.__stdout__
    _sys__stderr__ = sys.__stderr__
    _outs = {}
    _errs = {}
    
    def __init__(self, stdout=':memory:', stderr=':memory:', **options):
        """
        Options:
        
            stdout: str
                - if None, no capture
                - if ':memory:', capture into memory, accessible by `.out_str`
                - other value is considered as file path

            stderr: str
                - if None, no capture
                - if ':memory:', capture into memory, accessible by `.err_str`
                - if ':stdout:', then same as stdout
                - other value is considered as file path
        """
        self.stdout = stdout
        self.stderr = stderr
        self.options = options

        self.out_path = None
        self.err_path = None
        self.out_file = None
        self.err_file = None
        self.proc = None

        self._capturing = False
    
    @property
    def out_str(self) -> str:
        if self.out_path:
            with self.out_path.open() as f:
                return f.read()
        elif self.out_file:
            return self.out_file.read()
    
    @property
    def err_str(self) -> str:
        if self.err_path:
            with self.err_path.open() as f:
                return f.read()
        elif self.err_file and self.err_file is not self.out_file:
            return self.err_file.read()
    
    @property
    def out(self):
        return _OutWrapper(self, lambda: getattr(self, 'out_path'), lambda: getattr(self, 'out_file'))
    
    @property
    def err(self):
        return _OutWrapper(self, lambda: getattr(self, 'err_path'), lambda: getattr(self, 'err_file'))
    
    def popen(self, *args, **kwargs):
        self.out_path, self.out_file = _setup_out(self.stdout, 'stdout', kwargs)
        self.err_path, self.err_file = _setup_out(self.stderr, 'stderr', kwargs)
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
        self._capturing = True

        with contextlib.ExitStack() as stack:
            if self.proc:
                self._collect_outputs()
                self.proc.wait()
            else:
                if self.options.get('should_enable_disable', True):
                    stack.enter_context(_proxied())

                self.out_path, self.out_file = _setup_out(self.stdout, 'stdout')
                self.err_path, self.err_file = _setup_out(self.stderr, 'stderr')
                if self.stderr == ':stdout:':
                    self.err_file = self.out_file

                stack.enter_context(_redirected(Capture._outs, self.out_file))
                stack.enter_context(_redirected(Capture._errs, self.err_file))

            stack.enter_context(_closing(self.out_file))
            stack.enter_context(_closing(self.err_file))

            yield self

        self._capturing = False
    
    def _collect_outputs(self):
        fds = []
        fd_mapping = {}
        
        def setup_out(src, dst):
            fileno = src.fileno()
            fds.append(fileno)
            fd_mapping[fileno] = (src, dst)
        
        if self.out_file and isinstance(self.out_file, _Output):
            setup_out(self.proc.stdout, self.out_file)
        if self.err_file and isinstance(self.err_file, _Output):
            setup_out(self.proc.stderr, self.err_file)

        try:
            while fds:
                for fd in select.select(fds, [], [])[0]:
                    src, dst = fd_mapping[fd]
                    line = src.readline()
                    if not line:
                        fds.remove(fd)
                        continue
                    dst.write(line)
        except KeyboardInterrupt:
            pass

    @staticmethod
    def enable_proxy():
        if not Capture._enabled:
            # save original stdout/stderr
            Capture._sys_stdout = sys.stdout
            Capture._sys_stderr = sys.stderr
            Capture._sys__stdout__ = sys.__stdout__
            Capture._sys__stderr__ = sys.__stderr__

            # replace with thread local proxies
            sys.stdout = LocalProxy(lambda: Capture._outs.get(threading.get_ident(), Capture._sys_stdout))
            sys.stderr = LocalProxy(lambda: Capture._errs.get(threading.get_ident(), Capture._sys_stderr))
            sys.__stdout__ = LocalProxy(lambda: Capture._outs.get(threading.get_ident(), Capture._sys__stdout__))
            sys.__stderr__ = LocalProxy(lambda: Capture._errs.get(threading.get_ident(), Capture._sys__stderr__))

            Capture._enabled = True

    @staticmethod
    def disable_proxy():
        if Capture._enabled:
            # restore original stdout/stderr
            sys.stdout = Capture._sys_stdout
            sys.stderr = Capture._sys_stderr
            sys.__stdout__ = Capture._sys__stdout__
            sys.__stderr__ = Capture._sys__stderr__

            Capture._enabled = False


class _Output:
    
    def __init__(self, contents=None):
        self.contents = contents or []
    
    def read(self):
        return ''.join(self.contents)
    
    def write(self, content: str):
        self.contents.append(content)
    
    def close(self):
        pass
    
    async def readline(self):
        self._lines = self.read().split('\n')[:-1]
        if self._i_line >= len(self._lines):
            return None
        line = self._lines[self._i_line]
        self._i_line += 1
        return line + '\n'
    
    def _as_readable_file(self):
        ret = _Output(self.contents)
        ret._prepare_read()
        return ret
    
    def _prepare_read(self):
        self._lines = self.read().split('\n')
        self._i_line = 0


class _OutWrapper:
    
    def __init__(self, capture, get_out_path, get_out_file):
        self.capture = capture
        self.get_out_path = get_out_path
        self.get_out_file = get_out_file

    async def iter_async(self, follow: bool = False):
        async with self._use_out_file_async(follow=follow) as f:
            if not f:
                return

            if follow:
                prev_capturing = self.capture._capturing

                while True:
                    line = await f.readline()
                    if line:
                        yield line
                    else:
                        await asyncio.sleep(0.01)

                    if prev_capturing and not self.capture._capturing:
                        break
            else:
                async for line in f:
                    yield line
    
    @property
    def out_path(self):
        return self.get_out_path()
    
    @property
    def out_file(self):
        return self.get_out_file()
    
    @contextlib.asynccontextmanager
    async def _use_out_file_async(self, follow: bool = False):
        while True:
            yielded = True

            if self.out_path:
                async with aiofiles.open(self.out_path) as f:
                    yield f
            elif self.out_file:
                yield self.out_file._as_readable_file()
            else:
                yielded = False
            
            if follow and not yielded:
                await asyncio.sleep(0.01)
                continue

            break


def _setup_out(out, name, kwargs={}):
    out_file = None
    out_path = None

    if out is None:
        kwargs[name] = None
    elif out == ':memory:':
        kwargs[name] = subprocess.PIPE
        out_file = _Output()
    elif out == ':stderr:':
        kwargs[name] = subprocess.STDERR
    elif out == ':stdout:':
        kwargs[name] = subprocess.STDOUT
    else:
        out_path = Path(out).expanduser()
        kwargs[name] = out_file = out_path.open('w', buffering=1)
    
    return out_path, out_file


@contextlib.contextmanager
def _proxied():
    Capture.enable_proxy()
    yield
    Capture.disable_proxy()


@contextlib.contextmanager
def _redirected(outs, out):
    key = threading.get_ident()
    if out:
        outs[key] = out
    yield
    outs.pop(key, None)


@contextlib.contextmanager
def _closing(thing):
    yield
    if thing:
        thing.close()
