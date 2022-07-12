import json
from typing import Callable


class Runnable:

    @staticmethod
    def make(spec: dict):
        cls = None
        kwargs = {
            'args': spec.get('args'),
            'kwargs': spec.get('kwargs'),
        }
        if spec.get('cmd'):
            env = spec.get('env')
            if env and isinstance(env, str):
                env = json.loads(env)
            cls = ProcessRunnable
            kwargs.update({
                'cmd': spec.get('cmd'),
                'cwd': spec.get('cwd'),
                'env': env,
            })
        elif spec.get('script'):
            cls = ScriptRunnable
            kwargs.update({
                'script': spec.get('script'),
            })
        else:
            raise RuntimeError(f'invalid runnable spec: {spec}')
        return cls(**kwargs)


class ProcessRunnable:
    """
    {
        'cmd': 'for i in 1 2 3; echo $i; sleep 1; done',
        'cwd': '/home/fans656',
        'env': {'foo': 3, 'bar': 5},
    }
    """

    def __init__(
            self,
            cmd: str,
            cwd: str = None,
            env: dict = None,
            **__,
    ):
        pass


class ScriptRunnable:
    """
    {
        'script': '/home/fans656/t.py',
    }
    """

    def __init__(
            self,
            script: str,
            **__,
    ):
        pass


class ModuleRunnable:
    """
    {
        'module': 'quantix.pricer',
    }
    """

    def __init__(
            self,
            module: str,
            **__,
    ):
        pass


class FunctionRunnable:
    """
    {
        'func': <function>,
    }
    """

    def __init__(
            self,
            func: Callable,
            **__,
    ):
        pass
