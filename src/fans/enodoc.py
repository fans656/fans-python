import inspect
import importlib
from collections import deque


def enodoc(*args, **kwargs):
    """
    Generate documentation from code by decorations.
    """
    if args or kwargs:
        target = _make_target(args, kwargs)
        _doc.targets.append(target)
        #print('enodoc', args[0] if args else None)
        print('enodoc', target, args)
        return target.get('decorator')
    else:
        _doc.process()
    return _doc


def entry(file, follow = []):
    _doc.entry = {'file': file, 'follow': follow}
enodoc.entry = entry


def _make_target(args, kwargs):
    """
    Decorate function/method:

        @enodoc
        def do_something():
            pass

    Decorate function/method with configurations:

        @enodoc('init')
        def make_jobs():
            pass

    Decorate variables:

        enodoc(
            services, {},
            entry = True,
        )
    """
    target = {
        'type': None,
        'target_type': None,
    }
    if len(args) == 0:
        target['type'] = 'decorator_with_conf'
        target['target_type'] = 'variables'
        target['decorator'] = _make_func_decorator_with_conf(target)
    elif len(args) == 1:
        if callable(args[0]):
            target['type'] = 'decorator'
            target['target_type'] = 'func'
            target['target'] = args[0]
            target['decorator'] = _make_func_decorator(target)
        elif isinstance(args[0], str):
            target['type'] = 'decorator_with_conf'
            target['decorator'] = _make_func_decorator_with_conf(target, type = args[0])
        else:
            raise NotImplementedError()
    else:
        target['type'] = 'process_variables'
        target['target_type'] = 'variables'
        target['args'] = args
        target['kwargs'] = kwargs

    #if kwargs.get('entry'):
    #    stack = inspect.stack()
    #    caller_frame = stack[2]
    #    caller_module = inspect.getmodule(caller_frame[0])
    #    target['file'] = caller_module.__file__

    return target


class Doc:

    def __init__(self):
        self.targets = []
        self.out = []

    def process(self):
        for target in self.targets:
            self.process_target(target)

        que = deque([self.entry])
        while que:
            cur = que.popleft()
            self.out.append(f'{cur["file"]}')
            for nex in cur.get('follow', []):
                if isinstance(nex, type):
                    parse_class(nex)
                    nex = {
                        'file': nex.__module__,
                    }
                que.append(nex)
        #print(self.entry)

    def process_target(self, target):
        #print('process_target', target)
        #kwargs = target.get('kwargs')
        return

        conf = _make_conf(*args, **kwargs)
        match conf['target_type']:
            case 'variables':
                for i, arg in enumerate(args):
                    if i % 2 == 0:
                        if inspect.isclass(arg):
                            cls = arg
                        else:
                            cls = type(arg)
                        assert inspect.isclass(cls)
                        module = importlib.import_module(cls.__module__)
                        print(module)
                    else:
                        pass


_doc = Doc()


def _make_func_decorator(target, type = None):
    def _func_decorator(func):
        target['func'] = func
        print('_func_decorator', func)
        match type:
            case 'init':
                print('func', func)
                func.__enodoc__ = {
                    'type': type,
                }
        return func
    return _func_decorator


def _make_func_decorator_with_conf(target, type = None):
    def _func_decorator_with_conf(*args, **kwargs):
        target['args'] = args
        target['kwargs'] = kwargs
        return _make_func_decorator(target, type = type)
    return _func_decorator_with_conf


def obj_to_str(obj):
    print(obj.__class__.__module__)
    return str(obj.__module__)


def parse_class(cls):
    print('parse_class', cls)
    for method in dir(cls):
        doc = getattr(method, '__enodoc__', None)
        if doc:
            print('hi', method)
