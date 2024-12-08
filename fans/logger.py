import os
import sys
import json
import logging
import traceback
import threading
from pathlib import Path
from collections.abc import Mapping
from typing import Callable

import pytz
from fans import errors
from fans.bunch import bunch
from fans.fn import noop


_setup_done = False
timezone = pytz.timezone('Asia/Shanghai')


def get_logger(name):
    if not _setup_done:
        setup_logging()
    return Logger(logging.getLogger(name))


def set_log_level(level):
    logging.root.setLevel(level)


def setup_logging(module_levels = {}):
    global _setup_done
    root = logging.root
    # TODO: library should not set level, only app should
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)

    Logger.reset_handlers()

    run_path = os.environ.get('LOGDIR')
    if run_path:
        handler = Handler(Path(run_path))
        root.addHandler(handler)

    _setup_done = True


class ContextManager:

    def __init__(self):
        self.threadlocal = threading.local()

    @property
    def contexts(self):
        if not hasattr(self.threadlocal, 'contexts'):
            self.threadlocal.contexts = []
        return self.threadlocal.contexts

    def push(self, context):
        self.contexts.append(context)

    def pop(self):
        contexts = self.contexts
        if contexts:
            contexts.pop()

    def top(self):
        contexts = self.contexts
        return contexts[-1] if contexts else None

    def create(self, *args, **kwargs):
        return Context(self, *args, **kwargs)


class Context:

    def __init__(
            self,
            context_manager: ContextManager,
            on_progress: Callable[[str, dict], None] = None,
            on_notify: Callable[[dict], None] = None,
            *,
            __initial: bool = False,
            **kwargs,
    ):
        self.__context_manager = context_manager
        self.__kwargs = kwargs

        parent = self.__context_manager.top()
        if parent:
            self.on_progress = on_progress or parent.on_progress
            self.on_notify = on_notify or parent.on_notify
            for key, value in parent.__kwargs.items():
                setattr(self, key, kwargs.get(key, value))
        else:
            self.on_progress = on_progress
            self.on_notify = on_notify
            for key, value in kwargs.items():
                setattr(self, key, value)

    def __enter__(self):
        self.__context_manager.push(self)
        return self

    def __exit__(self, *_, **__):
        self.__context_manager.pop()

    @property
    def __parent(self) -> 'Context':
        return


context_manager = ContextManager()


class Logger:

    # TODO: not reset, but replace (or add) a stream handler using latest sys.stderr
    # to support thread output capture
    @staticmethod
    def reset_handlers(module_levels = {}):
        root = logging.root

        for name, level in module_levels.items():
            logger = logging.getLogger(name)
            logger.setLevel(level)

        root.handlers.clear()
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s | %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)

    def __init__(self, logger):
        self.logger = logger

    #@property
    #def guard(self):
    #    from quantix.common.guard import Guard
    #    return Guard(logger)

    def timing(self, *args, **kwargs):
        from fans.timing import timing
        kwargs.setdefault('logger', self.logger)
        return timing(*args, **kwargs)

    # TODO: remove old impl returning `progress` instance
    #def progress(self, *args, **kwargs):
    #    from fans.progress import progress
    #    kwargs.setdefault('logger', self.logger)
    #    return progress(*args, **kwargs)

    def context(self, *args, **kwargs) -> Context:
        """
        Usage:

            logger = get_logger(__name__)

            with logger.context(on_progress = lambda message, data: ...):
                logger.progress('hello')

            # OR to get current context

            context = logger.context()
        """
        if args or kwargs:
            return context_manager.create(*args, **kwargs)
        else:
            return context_manager.top() or context_manager.create(
                on_progress = noop,
                on_notify = noop,
            )

    def progress(self, message: str = None, data: dict = None):
        self.info(message)
        self.context().on_progress(message, data)

    def notify(self, data: dict):
        self.context().on_notify(data)

    def exception(self, message, data = None, exc_cls = None):
        if not data:
            self.error(f'{message} | {data}')
        else:
            self.error(traceback.format_exc())
            self.error(f'{message}')
        exc_cls = exc_cls or Exception
        return exc_cls(message, data)

    def stop(self, *args, **kwargs):
        # TODO: do not print the error
        return self.exception(*args, **{'exc_cls': errors.Stop, **kwargs})

    def fail(self, *args, **kwargs):
        return self.exception(*args, **{'exc_cls': errors.Fail, **kwargs})

    def __getattr__(self, key):
        return getattr(self.logger, key)


class Handler(logging.Handler):

    def __init__(
            self,
            run_path,
            info_log_fname = 'info.log',
            warning_log_fname = 'warning.log',
            error_log_fname = 'error.log',
            data_log_fname = 'data.log',
    ):
        super().__init__()
        self.run_path = run_path

        self.info_fpath = run_path / info_log_fname
        self.warning_fpath = run_path / warning_log_fname
        self.error_fpath = run_path / error_log_fname
        self.data_fpath = run_path / data_log_fname

        self.files = {
            'info': self.info_fpath.open('w+'),
            'warning': self.warning_fpath.open('w+'),
            'error': self.error_fpath.open('w+'),
            'data': self.data_fpath.open('w+'),
        }

    def emit(self, record):
        levelno = record.levelno
        data_only = False
        if levelno <= logging.INFO:
            stream = self.files['info']
        elif levelno <= logging.WARNING:
            stream = self.files['warning']
        elif levelno <= logging.ERROR:
            stream = self.files['error']
        else:
            stream = self.files['data']
            data_only = True

        data = record.msg
        if data_only:
            item = data
        else:
            if not isinstance(data, Mapping):
                data = {'data': data}
            item = {
                '_date': datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S'),
                '_level': record.levelname,
                '_module': record.module,
                **data,
            }
            if record.exc_info:
                item.update({
                    '_exception': str(record.exc_info[1]),
                    '_traceback': ' '.join(traceback.format_exception(*record.exc_info))
                })

        stream.write(json.dumps(item, ensure_ascii = False) + '\n')
        stream.flush()

    def close(self):
        for f in self.files.values():
            f.close()
