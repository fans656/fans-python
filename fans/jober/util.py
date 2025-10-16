import sys
import werkzeug.local
import threading


enabled = False
output_targets = {}
stdout = sys.stdout
stderr = sys.stderr
__stdout__ = sys.__stdout__
__stderr__ = sys.__stderr__


def enable_proxy():
    global enabled
    if not enabled:
        sys.stdout = werkzeug.local.LocalProxy(_make_output_getter(stdout))
        sys.stderr = werkzeug.local.LocalProxy(_make_output_getter(stderr))
        sys.__stdout__ = werkzeug.local.LocalProxy(_make_output_getter(__stdout__))
        sys.__stderr__ = werkzeug.local.LocalProxy(_make_output_getter(__stderr__))
        enabled = True


def disable_proxy():
    global enabled
    if enabled:
        sys.stdout = stdout
        sys.stderr = stderr
        sys.__stdout__ = __stdout__
        sys.__stderr__ = __stderr__
        enabled = False


def redirect_to(output):
    output_targets[threading.get_ident()] = output


def _make_output_getter(default):
    return lambda: output_targets.get(threading.get_ident(), default)
