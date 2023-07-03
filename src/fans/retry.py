# TODO: implement retry.call (see stome.storage.s3.storage)
# TODO: try use generator "yield" for more powerful control
import time
import inspect
import traceback
import itertools
import functools

from fans.bunch import bunch
from fans.fn import noop
from fans.logger import get_logger


logger = get_logger(__name__)


def retry(*args, **conf):
    """
    Retry function if failed.

    Simple inline usage:

        retry(func) # keep retrying

        retry(func, 3) # retry with at most 3 executions

        retry(func, wait = True) # wait 1 second before next retry

        retry(func, wait = 10) # wait 10 seconds before next retry

        retry(func, wait = lambda c: 2 ** c.i) # dynamic wait seconds

    More control using generator block:

        @retry
        def result():
            value = get_something()
            if not value:
                yield # indicate intent to retry
            return value

        print(result) # result will be a variable containing block return value

    Block execution can also have config:

        @retry(3, wait = True)
        def result():
            pass

    Function parameters:

        func: callable - function to call with retry [optional][positional]
        times: int - most times of executions, no limit by default [optional][positional|keyword]
        wait - control wait before next retry
            : bool - if True wait 1 second before next retry, no wait otherwise
            : int|float - wait <wait> seconds before next retry
            : Callable[[Context], int|float] - get number of seconds to wait before next retry

    Context (provided to `wait`) attributes:
        i: int - number of retries done, range from 0 to execution limits - 1
        exc: BaseException - exception raised (None if no exception)
    """

    conf = _normalize_arguments(*args, **conf)
    func = conf.get('func')
    if func:
        controller = Controller(**conf)
        if inspect.isgeneratorfunction(func):
            return _retry_gene(func, controller)
        else:
            return _retry_func(func, controller)
    else:
        return functools.partial(retry, **conf)


def _retry_gene(make_block, controller):
    for i_retry in controller.retry_indexes:
        try:
            intent = next(make_block()) # got yielded retry intent
            exc = None
        except StopIteration as e:
            return e.value # successfully got block return value, return
        except Exception as e:
            intent = None
            exc = e
            if i_retry + 1 == controller.n_calls:
                controller.fail(i_retry)
                raise
            controller.on_exception(exc)

        ctx = controller.make_context(i_retry = i_retry, exc = exc)

        # TODO: handle more types of intent (currently intent value is ignored)

        controller.wait(ctx)


def _retry_func(func, controller):
    for i_retry in controller.retry_indexes:
        try:
            ret = func()
        except Exception as exc:
            if i_retry + 1 == controller.n_calls:
                controller.fail(i_retry)
                raise
            ctx = controller.make_context(i_retry = i_retry, exc = exc)
            controller.on_exception(ctx)
            controller.wait(ctx)
        else:
            return ret


def _normalize_arguments(*args, **conf):
    invalid_arguments = None
    match len(args):
        case 0: # retry(wait = True)
            pass
        case 1:
            if callable(args[0]): # retry(func, wait = True)
                conf.update({'func': args[0]})
            elif isinstance(args[0], int): # @retry(3, wait = True)
                conf.update({'times': args[0]})
            else:
                invalid_arguments = True
        case 2:
            if callable(args[0]): # retry(func, 3, wait = True)
                conf.update({'func': args[0], 'times': args[1]})
            else:
                invalid_arguments = True
        case _:
            invalid_arguments = True
    if invalid_arguments:
        raise ValueError(f'invalid arguments: {args} {conf}')
    return conf


class Controller:

    def __init__(
            self,
            func = None,
            times = None,
            wait = None,
            log = None,
    ):
        self.should_log_wait = False
        self.should_log_trace = False
        self.should_log_fail = False

        if times:
            self.n_calls = times
            self.retry_indexes = range(times)
        else:
            self.n_calls = float('inf')
            self.retry_indexes = itertools.count()

        if log:
            if isinstance(log, str):
                topics = set(log.split(','))
                if 'wait' in topics:
                    self.should_log_wait = True
                if 'trace' in topics:
                    self.should_log_trace = True
                if 'fail' in topics:
                    self.should_log_fail = True
            else:
                raise ValueError(f'invalid "log" value: "{log}"')

        if wait is None:
            self.get_wait_seconds = lambda _: None
        elif isinstance(wait, bool):
            self.get_wait_seconds = lambda _: int(wait)
        elif isinstance(wait, (int, float)):
            self.get_wait_seconds = lambda _: wait
        elif callable(wait):
            self.get_wait_seconds = wait
        else:
            raise ValueError(f'invalid "wait" value: "{wait}"')

    def fail(self, i_retry):
        if self.should_log_fail:
            logger.info(f'retry failed upon reaching limit {i_retry}/{self.n_calls - 1}')

    def wait(self, ctx):
        seconds = self.get_wait_seconds(ctx)
        if self.should_log_wait and seconds:
            logger.info(f'wait {seconds}s before {ctx.i + 1}/{self.n_calls - 1} retry')
        if seconds:
            time.sleep(seconds)

    def on_exception(self, exc):
        if self.should_log_trace:
            logger.info(traceback.format_exc())

    def make_context(self, *, i_retry, exc):
        return bunch(
            i = i_retry,
            exc = exc,
        )
