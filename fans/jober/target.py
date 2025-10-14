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


class Target:
    """
    Wrapper around different type of executable.
    
    Target can be called:
    
        target = Target.make('date')
        target()
    
    can bind args:
    
        target = Target.make('ls')
        bound_target = target.bind('-lh')
        bound_target()
    
    can specify execution options:
    
        target = Target.make(func, process=True, encoding='gbk')
        target()
    
    Following target types are supported:
    - external executable
        - command: execute external binary, e.g. `Target.make('ls')`
    - python executable
        - python_script: execute a python script using same intepreter as current process, e.g. `Target.make('crawl.py')`
        - python_module: execute a python module using same intepreter as current process, e.g. `Target.make('crawl.prices')`
    - python callable
        - python_callable: execute python callable, e.g. `Target.make(func)`
        - python_script_callable: load callable from python script, e.g. `Target.make('crawl.py:main')`
        - python_module_callable: load callable from python module, e.g. `Target.make('crawl.prices:main')`
    """

    class Type:

        command = 'command'
        python_script = 'python_script'
        python_module = 'python_module'
        python_callable = 'python_callable'
        python_script_callable = 'python_script_callable'
        python_module_callable = 'python_module_callable'

    @staticmethod
    def make(source: Union[Callable, str, List[str]], args = (), kwargs  = {}, **opts):
        impl_cls = _get_impl_cls(source, **opts)
        return impl_cls(source, args, kwargs, opts=opts)

    def __init__(self, source, args, kwargs, opts):
        self.source = source
        self.args = args
        self.kwargs = kwargs
        self.opts = opts

    def __call__(self):
        self._prepare_call()
        return self._do_call()
    
    def bind(self, args, kwargs):
        return Target.make(self.source, args, kwargs, **self.opts)

    def _prepare_call(self):
        pass

    def _do_call(self):
        raise NotImplementedError()
    
    @property
    def cwd(self) -> Path:
        return Path(self.opts.get('cwd') or os.getcwd())
    
    def _popen(self, cmd: str|list[str]):
        proc = subprocess.Popen(
            cmd,
            cwd=str(self.cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # redirect to stdout
            text=True,
            encoding=self.opts.get('encoding', 'utf-8'),
            bufsize=1,  # line buffered
            errors='replace',
            shell=self.opts.get('shell', False),
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

    type = Target.Type.command

    def _do_call(self):
        cmd = self.source
        if not self.opts.get('shell'):
            if isinstance(cmd, str):
                cmd = shlex.split(cmd)
            cmd = [*cmd, *self.args, *_to_cmdline_options(self.kwargs)]
        return self._popen(cmd)


class PythonExecutableTarget(Target):

    def _do_call(self):
        cmd = [sys.executable, *self.get_execute_args(), *self.args, *_to_cmdline_options(self.kwargs)]
        return self._popen(cmd)


class PythonScriptTarget(PythonExecutableTarget):

    type = Target.Type.python_script

    def get_execute_args(self):
        return (self.source,)


class PythonModuleTarget(PythonExecutableTarget):

    type = Target.Type.python_module

    def get_execute_args(self):
        return ('-m', self.source,)


class CallableTarget(Target):

    def _prepare_call(self):
        self.func = None

    def _do_call(self):
        if self.opts.get('process'):
            return self._execute_func_in_process()
        else:
            return self.func(*self.args, **self.kwargs)
    
    def _execute_func_in_process(self):
        raise NotImplementedError()


class PythonCallableTarget(CallableTarget):

    type = Target.Type.python_callable

    def _prepare_call(self):
        self.func = self.source
    
    def _execute_func_in_process(self):
        data = (self.func, self.args, self.kwargs)
        data_text = base64.b64encode(pickle.dumps(data)).decode("utf-8")
        args = [
            sys.executable,
            '-c',
            (
                f'import pickle, base64;'
                f'func, args, kwargs = pickle.loads(base64.b64decode("{data_text}"));'
                f'func(*args, **kwargs)'
            ),
        ]
        return self._popen(args)


class PythonScriptCallableTarget(CallableTarget):

    type = Target.Type.python_script_callable

    def _prepare_call(self):
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

    type = Target.Type.python_module_callable

    def _prepare_call(self):
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


def _to_cmdline_options(kvs: dict):
    def gen():
        for key, value in kvs.items():
            yield f'--{key}'
            yield f'{value}'
    return list(gen())


def _get_impl_cls(source: str, **opts):
    if opts.get('shell') or isinstance(source, list):
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
                    # e.g. "crawl.py:main"
                    return PythonScriptCallableTarget
                else:
                    # e.g. "crawl.prices:main"
                    return PythonModuleCallableTarget
            elif source.endswith('.py'):
                # e.g. "crawl.py"
                return PythonScriptTarget
            elif not source.startswith('.') and '.' in source:
                # e.g. "crawl.prices"
                return PythonModuleTarget
        return CommandTarget
    else:
        raise ValueError(f'invalid target: source="{source}" opts={opts}')
