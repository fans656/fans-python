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


class TargetType:

    command = 'command'
    python_callable = 'python_callable'
    python_script_callable = 'python_script_callable'
    python_module_callable = 'python_module_callable'
    python_script = 'python_script'
    python_module = 'python_module'


class Target:

    @classmethod
    def make(
            cls,
            source: Union[Callable, str, List[str]],
            args = (),
            kwargs  = {},
            **extras,
    ):
        if extras.get('shell'):
            impl = CommandTarget
        elif callable(source):
            impl = PythonCallableTarget
        elif isinstance(source, str):
            parts = shlex.split(source)
            if not parts:
                raise ValueError(f'invalid source "{source}"')
            impl = None
            if len(parts) == 1:
                if ':' in source:
                    domain_str, func_str = source.split(':')
                    if domain_str.endswith('.py'):
                        impl = PythonScriptCallableTarget
                    else:
                        impl = PythonModuleCallableTarget
                elif source.endswith('.py'):
                    impl = PythonScriptTarget
                elif '.' in source:
                    impl = PythonModuleTarget
            if impl is None:
                impl = CommandTarget
        elif isinstance(source, list):
            impl = CommandTarget
        else:
            raise ValueError(f'invalid target source "{source}"')

        return impl(source, args, kwargs, extras=extras)

    def __init__(self, source, args, kwargs, extras):
        self.source = source
        self.args = args
        self.kwargs = kwargs
        self.extras = extras
    
    def bind(self, args, kwargs):
        return Target.make(self.source, args, kwargs, **self.extras)

    def __call__(self):
        self.prepare_call()
        return self.do_call()

    def prepare_call(self):
        pass

    def do_call(self):
        raise NotImplementedError()
    
    @property
    def kwargs_as_cmdline_options(self):
        def gen():
            for key, value in self.kwargs.items():
                yield f'--{key}'
                yield f'{value}'
        return list(gen())
    
    @property
    def cwd(self) -> Path:
        return Path(self.extras.get('cwd') or os.getcwd())
    
    def _popen(self, cmd: str|list[str]):
        proc = subprocess.Popen(
            cmd,
            cwd=str(self.cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # redirect to stdout
            text=True,
            encoding=self.extras.get('encoding', 'utf-8'),
            bufsize=1,  # line buffered
            errors='replace',
            shell=self.extras.get('shell', False),
        )
        try:
            for line in iter(proc.stdout.readline, ''):
                print(line, end='')
        except KeyboardInterrupt:
            pass
        finally:
            proc.wait()
        
        return proc.returncode


class CommandTarget(Target):

    type = TargetType.command

    def do_call(self):
        cmd = self.source
        if not self.extras.get('shell'):
            if isinstance(cmd, str):
                cmd = shlex.split(cmd)
            cmd = [*cmd, *self.args, *self.kwargs_as_cmdline_options]
        return self._popen(cmd)


class CallableTarget(Target):

    def prepare_call(self):
        self.func = None

    def do_call(self):
        if self.extras.get('process'):
            return self._execute_func_in_process()
        else:
            return self.func(*self.args, **self.kwargs)
    
    def _execute_func_in_process(self):
        raise NotImplementedError()


class PythonCallableTarget(CallableTarget):

    type = TargetType.python_callable

    def prepare_call(self):
        self.func = self.source
    
    def _execute_func_in_process(self):
        data = (self.func, self.args, self.kwargs)
        data_text = base64.b64encode(pickle.dumps(data)).decode("utf-8")
        return self._popen([
            sys.executable,
            '-c',
            (
                f'import pickle, base64;'
                f'func, args, kwargs = pickle.loads(base64.b64decode("{data_text}"));'
                f'func(*args, **kwargs)'
            ),
        ])


class PythonScriptCallableTarget(CallableTarget):

    type = TargetType.python_script_callable

    def prepare_call(self):
        path, func_name = self.source.split(':')
        path = self.cwd / path
        name = hashlib.md5(str(path).encode('utf-8')).hexdigest()
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.func = getattr(module, func_name)
        
        self.path = path
        self.func_name = func_name
    
    def _execute_func_in_process(self):
        data = (self.args, self.kwargs)
        data_text = base64.b64encode(pickle.dumps(data)).decode("utf-8")
        self._popen([
            sys.executable,
            '-c',
            (
                f'import pickle, base64;'
                f'import importlib.util;'
                f'spec = importlib.util.spec_from_file_location("", "{self.path}");'
                f'module = importlib.util.module_from_spec(spec);'
                f'spec.loader.exec_module(module);'
                f'func = getattr(module, "{self.func_name}");'
                f'args, kwargs = pickle.loads(base64.b64decode("{data_text}"));'
                f'func(*args, **kwargs);'
            ),
        ])


class PythonModuleCallableTarget(CallableTarget):

    type = TargetType.python_module_callable

    def prepare_call(self):
        module_name, func_name = self.source.split(':')
        spec = importlib.util.find_spec(module_name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.func = getattr(module, func_name)
        
        self.module_name = module_name
        self.func_name = func_name
    
    def _execute_func_in_process(self):
        data = (self.args, self.kwargs)
        data_text = base64.b64encode(pickle.dumps(data)).decode("utf-8")
        self._popen([
            sys.executable,
            '-c',
            (
                f'import pickle, base64;'
                f'from {self.module_name} import {self.func_name};'
                f'args, kwargs = pickle.loads(base64.b64decode("{data_text}"));'
                f'{self.func_name}(*args, **kwargs);'
            ),
        ])


class PythonExecutableTarget(Target):

    def do_call(self):
        cmd = [sys.executable, *self.get_execute_args(), *self.args, *self.kwargs_as_cmdline_options]
        return self._popen(cmd)


class PythonScriptTarget(PythonExecutableTarget):

    type = TargetType.python_script

    def get_execute_args(self):
        return (self.source,)


class PythonModuleTarget(PythonExecutableTarget):

    type = TargetType.python_module

    def get_execute_args(self):
        return ('-m', self.source,)
