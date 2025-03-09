import multiprocessing

import pytest

from fans.jober import Jober


# NOTE: fix following error
#     RuntimeError: A SemLock created in a fork context is being shared with a
#     process in a spawn context. This is not supported. Please use the same
#     context to create multiprocessing objects and Process.
multiprocessing.set_start_method("spawn", force=True)


@pytest.fixture
def jober():
    return Jober()
