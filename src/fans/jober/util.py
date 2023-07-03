import io
import sys
import werkzeug.local
import threading

from fans.logger import Logger

from .event import RunEventer


enabled = False
thread_proxies = {}
stdout = sys.stdout
stderr = sys.stderr
__stdout__ = sys.__stdout__
__stderr__ = sys.__stderr__


def enable_proxy():
    global enabled
    if not enabled:
        sys.stdout = werkzeug.local.LocalProxy(_get_proxy(stdout))
        sys.stderr = werkzeug.local.LocalProxy(_get_proxy(stderr))
        sys.__stdout__ = werkzeug.local.LocalProxy(_get_proxy(__stdout__))
        sys.__stderr__ = werkzeug.local.LocalProxy(_get_proxy(__stderr__))
        enabled = True


def disable_proxy():
    global enabled
    if enabled:
        sys.stdout = stdout
        sys.stderr = stderr
        sys.__stdout__ = __stdout__
        sys.__stderr__ = __stderr__
        enabled = False


def _get_proxy(obj):
    return lambda: thread_proxies.get(threading.get_ident(), obj)


def redirect(
        enable = True,
        queue = None,
        job_id = None,
        run_id = None,
        module_logging_levels = {},
):
    if enable:
        enable_proxy()
    ident = threading.get_ident()
    output = Output()
    output.run_eventer = RunEventer(job_id = job_id, run_id = run_id)
    output.queue = queue
    thread_proxies[ident] = output
    Logger.reset_handlers(module_levels = module_logging_levels)
    return thread_proxies[ident]


class Output:

    def write(self, string):
        if self.queue and self.run_eventer:
            self.queue.put(self.run_eventer.output(string))
