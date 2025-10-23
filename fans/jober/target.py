import os
import sys
import shlex
import runpy
import base64
import pickle
import hashlib
import subprocess
import importlib.util
from pathlib import Path
from typing import Union, Callable, List, Iterable

from fans.bunch import bunch
from fans.jober.capture import Capture


class Target:
    """
    Wraps executable of different types (command, callable, etc).
    
    Target can be called:
    
        target = Target.make('date')
        target()
    
    can specify args:

        target = Target.make('ls', args=['-l'])
        target()

    can clone with modified args:
    
        target = Target.make('ls')
        bound_target = target.clone(args=['-lh'])
        bound_target()

    can specify execution options:
    
        target = Target.make(func, process=True, encoding='gbk')
        target()
    """

    class Type:

        # external executable
        command = 'command'                                 # e.g. `Target.make('ls')`

        # python executable
        python_script = 'python_script'                     # e.g. `Target.make('crawl.py')`
        python_module = 'python_module'                     # e.g. `Target.make('crawl.prices')`

        # python callable
        python_callable = 'python_callable'                 # e.g. `Target.make(func)`
        python_script_callable = 'python_script_callable'   # e.g. `Target.make('crawl.py:main')`
        python_module_callable = 'python_module_callable'   # e.g. `Target.make('crawl.prices:main')`

    @staticmethod
    def make(source, args=(), kwargs={}, **options):
        impl_cls = _get_impl_cls(source, **options)
        return impl_cls(source, args=args, kwargs=kwargs, **options)

    def __init__(self, source, args=(), kwargs={}, **options):
        """
        Availble options:
        
            process: bool - whether execute callable target in process instead

            stdout: str|None - stdout capture, see Capture
            stderr: str|None - stderr capture, see Capture

            shell: bool - argument for Popen, defaults to False
            text: bool - argument for Popen, defaults to True
            encoding: str - argument for Popen, defaults to 'utf-8'
            bufsize: int - argument for Popen, defaults to 1
            errors: str - argument for Popen, defaults to 'replace'
        """
        self.source = source
        self.args = args
        self.kwargs = kwargs
        self.options = options
        
        self.capture = None

    def __call__(self):
        raise NotImplementedError()
    
    def clone(self, args=None, kwargs=None, **options):
        if args is None:
            args = self.args
        
        if kwargs is None:
            kwargs = self.kwargs

        _options = self.options
        if options:
            _options = dict(_options)
            _options.update(options)

        return Target.make(self.source, args, kwargs, **_options)
    
    @property
    def cwd(self):
        return Path(self.options.get('cwd') or os.getcwd()).expanduser()
    
    def _run_in_place(self, func):
        with (self.capture or Capture(**self.options)):
            return func(*self.args, **self.kwargs)

    def _run_in_process(self, cmd: str|list[str]):
        options = self.options
        with (self.capture or Capture(**options)).popen(
            cmd,
            cwd=str(self.cwd),
            shell=options.get('shell', False),
            text=options.get('text', True),
            encoding=options.get('encoding', 'utf-8'),
            bufsize=options.get('bufsize', 1),
            errors=options.get('errors', 'replace'),
        ) as capture:
            return capture.proc.returncode


class CommandTarget(Target):

    type = Target.Type.command

    def __call__(self):
        cmd = self.source

        if not self.options.get('shell'):
            if isinstance(cmd, str):
                cmd = shlex.split(cmd)
            cmd = [*cmd, *_to_cmdline_options(self.args, self.kwargs)]

        return self._run_in_process(cmd)


class PythonScriptTarget(Target):

    type = Target.Type.python_script
    
    def __call__(self):
        return self._run_in_process([
            sys.executable,
            self.source,
            *_to_cmdline_options(self.args, self.kwargs),
        ])


class PythonModuleTarget(Target):

    type = Target.Type.python_module
    
    def __call__(self):
        return self._run_in_process([
            sys.executable,
            '-m',
            self.source,
            *_to_cmdline_options(self.args, self.kwargs),
        ])


class PythonCallableTarget(Target):

    type = Target.Type.python_callable
    
    def __call__(self):
        if self.options.get('process'):
            return self._run_in_process([
                sys.executable,
                '-c',
                (
                    f'import pickle, base64;'
                    f'func, args, kwargs = {_serialize_converted(self.source, self.args, self.kwargs)};'
                    f'func(*args, **kwargs)'
                ),
            ])
        else:
            return self._run_in_place(self.source)


class PythonScriptCallableTarget(Target):

    type = Target.Type.python_script_callable

    def __call__(self):
        path, func_name = self.source.split(':')
        path = self.cwd / path

        if self.options.get('process'):
            return self._run_in_process([
                sys.executable,
                '-c',
                (
                    f'import pickle, base64, importlib.util;'
                    f'spec = importlib.util.spec_from_file_location("", "{path}");'
                    f'module = importlib.util.module_from_spec(spec);'
                    f'spec.loader.exec_module(module);'
                    f'func = getattr(module, "{func_name}");'
                    f'args, kwargs = {_serialize_converted(self.args, self.kwargs)};'
                    f'func(*args, **kwargs);'
                ),
            ])
        else:
            name = hashlib.md5(str(path).encode('utf-8')).hexdigest()
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            func = getattr(module, func_name)
            return self._run_in_place(func)


class PythonModuleCallableTarget(Target):

    type = Target.Type.python_module_callable

    def __call__(self):
        module_name, func_name = self.source.split(':')

        if self.options.get('process'):
            return self._run_in_process([
                sys.executable,
                '-c',
                (
                    f'import pickle, base64;'
                    f'from {module_name} import {func_name};'
                    f'args, kwargs = {_serialize_converted(self.args, self.kwargs)};'
                    f'{func_name}(*args, **kwargs);'
                ),
            ])
        else:
            spec = importlib.util.find_spec(module_name)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            func = getattr(module, func_name)
            return self._run_in_place(func)


def _to_cmdline_options(args, kwargs):
    def gen():
        for arg in args:
            yield arg
        for key, value in kwargs.items():
            yield f'--{key}'
            yield f'{value}'
    return list(gen())


def _get_impl_cls(source: Union[Callable, str, List[str]], **options):
    if options.get('shell') or isinstance(source, list):
        return CommandTarget
    elif callable(source):
        return PythonCallableTarget
    elif isinstance(source, str):
        parts = shlex.split(source)
        if not parts:
            raise ValueError(f'invalid source "{source}"')
        if len(parts) == 1:
            if ':' in source:
                domain_str, func_str = source.split(':')
                if domain_str.endswith('.py'):
                    return PythonScriptCallableTarget
                else:
                    return PythonModuleCallableTarget
            elif source.endswith('.py'):
                return PythonScriptTarget
            elif not source.startswith('.') and '.' in source:
                return PythonModuleTarget
        return CommandTarget
    else:
        raise ValueError(f'invalid target: source="{source}" options={options}')
    

def _reprint_proc_stdout(proc):
    try:
        for line in iter(proc.stdout.readline, ''):
            print(line, end='')
    except KeyboardInterrupt:
        pass


def _serialize_converted(*data):
    text = base64.b64encode(pickle.dumps(data)).decode("utf-8")
    return f"pickle.loads(base64.b64decode('{text}'))"
