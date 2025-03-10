import multiprocessing

import pytest

from fans.bunch import bunch
from fans.jober import Jober


# NOTE: fix following error
#     RuntimeError: A SemLock created in a fork context is being shared with a
#     process in a spawn context. This is not supported. Please use the same
#     context to create multiprocessing objects and Process.
multiprocessing.set_start_method("spawn", force=True)


@pytest.fixture
def jober():
    jober = Jober()
    jober.start()
    return jober


def echo_func(message: str):
    print(message)


def parametrized():
    confs = [
        {
            'target_type': 'func',
            'target': echo_func,
        },
        {
            'target_type': 'module',
            'target': 'fans.jober.tests.samples.echo',
        },
    ]
    return pytest.mark.parametrize('conf', map(bunch, confs))
