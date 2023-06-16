def make_sched(*args, **kwargs):
    from .apscheduler_sched import Sched
    return Sched(*args, **kwargs)
