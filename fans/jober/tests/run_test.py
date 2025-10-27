from fans.jober.target import Target
from fans.jober.run import Run


def test_collect_traceback():

    def func():
        raise RuntimeError('oops')

    target = Target.make(func)
    run = Run(target)
    run()
    assert run.status == 'error'
    assert 'oops' in run.trace
