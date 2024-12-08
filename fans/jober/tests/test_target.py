import multiprocessing as mp

import pytest
from fans.path import Path

from fans.jober.target import Target, TargetType


class Test_target_make:

    def test_func(self):
        target = Target.make(sample_func)
        assert target.type == TargetType.python_callable

    def test_callable(self):
        instance = sample_instance()
        target = Target.make(instance)
        assert target.type == TargetType.python_callable

    def test_module_func(self):
        target = Target.make('fans.jober.tests.test_target:sample_func')
        assert target.type == TargetType.python_module_callable
        target.prepare_call()
        assert target.func() == 'sample_func'

    def test_script_func(self):
        target = Target.make(f'{Path(__file__)}:sample_func')
        assert target.type == TargetType.python_script_callable
        target.prepare_call()
        assert target.func() == 'sample_func'

    def test_module(self):
        target = Target.make('fans.jober.tests.sample_target')
        assert target.type == TargetType.python_module
        proc = mp.Process(target = target.do_call)
        proc.start()
        proc.join()
        assert proc.exitcode == 56

    def test_script(self):
        target = Target.make(f'{Path(__file__).parent / "sample_target.py"}')
        assert target.type == TargetType.python_script
        proc = mp.Process(target = target.do_call)
        proc.start()
        proc.join()
        assert proc.exitcode == 56

    def test_cmd_str(self):
        target = Target.make('date')
        assert target.type == TargetType.command
        assert target.do_call() == 0

    def test_cmd_list(self):
        target = Target.make(['ls', '-lh'])
        assert target.type == TargetType.command
        assert target.do_call() == 0

    def test_invalid_target_value(self):
        with pytest.raises(ValueError):
            Target.make({})

    def test_invalid_str(self):
        with pytest.raises(ValueError):
            Target.make('')


def sample_func():
    return 'sample_func'


class sample_instance:

    def __call__(self):
        pass
