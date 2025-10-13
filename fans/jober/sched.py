import logging

import pytz
from fans.bunch import bunch
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger


class Sched:

    module_logging_levels = {'apscheduler': logging.WARNING}

    def __init__(
            self,
            *,
            n_threads: int,
            thread_pool_kwargs = {},
            timezone = 'Asia/Shanghai',
            **_,
    ):
        self._timezone = pytz.timezone(timezone)
        self._sched = BackgroundScheduler(
            executors={
                'default': {
                    'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                    'max_workers': n_threads,
                    'pool_kwargs': thread_pool_kwargs,
                },
            },
            timezone=self._timezone,
        )

    def start(self):
        self._sched.start()

    def stop(self):
        self._sched.shutdown()

    def run_singleshot(self, func, args=(), kwargs={}, **extra_kwargs):
        job = self._sched.add_job(
            func,
            args=args,
            kwargs=kwargs,
            **extra_kwargs,
        )

    def run_interval(self, func, interval: int|float, args=(), kwargs={}, **extra_kwargs):
        job = self._sched.add_job(
            func,
            args=args,
            kwargs=kwargs,
            trigger=IntervalTrigger(seconds=interval),
            **extra_kwargs,
        )

    def run_cron(self, func, crontab: str, args=(), kwargs={}, **extra_kwargs):
        trigger = CronTrigger.from_crontab(crontab, timezone=self._timezone)
        job = self._sched.add_job(
            func,
            args=args,
            kwargs=kwargs,
            trigger=trigger,
            **extra_kwargs,
        )
