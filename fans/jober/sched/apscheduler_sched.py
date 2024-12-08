import logging
import threading
import multiprocessing as mp

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_REMOVED

from .base import Base


class ApschedulerSched(Base):

    module_logging_levels = {'apscheduler': logging.WARNING}

    def __init__(
            self,
            *,
            n_threads: int,
            n_processes: int,
            thread_pool_kwargs = {},
            process_pool_kwargs = {},
            **_,
    ):
        self._sched = BackgroundScheduler(
            executors = {
                'thread': {
                    'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                    'max_workers': n_threads,
                    'pool_kwargs': thread_pool_kwargs,
                },
                'process': {
                    'class': 'apscheduler.executors.pool:ProcessPoolExecutor',
                    'max_workers': n_processes,
                    'pool_kwargs': process_pool_kwargs,
                },
            },
            timezone = pytz.timezone('Asia/Shanghai'),
        )

    def start(self):
        self._sched.start()

    def stop(self):
        self._sched.shutdown()

    def run_singleshot(self, func, args = (), kwargs = {}, mode = None):
        job = self._sched.add_job(
            func,
            args = args,
            kwargs = kwargs,
            executor = get_executor_by_mode(mode),
        )


def get_executor_by_mode(mode: str):
    match mode:
        case 'thread':
            return 'thread'
        case 'process':
            return 'process'
        case _:
            return 'thread'
