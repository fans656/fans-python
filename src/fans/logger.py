import os
import json
import logging
import traceback
from pathlib import Path
from collections.abc import Mapping

import pytz


_setup_done = False
timezone = pytz.timezone('Asia/Shanghai')


def get_logger(name):
    if not _setup_done:
        setup_logging()
    return Logger(logging.getLogger(name))


def set_level(level):
    logging.root.setLevel(level)


def setup_logging():
    global _setup_done
    root = logging.root
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)
    root.handlers.clear()

    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s | %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    run_path = os.environ.get('LOGDIR')
    if run_path:
        handler = Handler(Path(run_path))
        root.addHandler(handler)

    _setup_done = True


class Logger:

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

    def progress(self, *args, **kwargs):
        from fans.progress import progress
        kwargs.setdefault('logger', self.logger)
        return progress(*args, **kwargs)

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
