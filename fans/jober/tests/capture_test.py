import time
from pathlib import Path

from fans.jober import Jober


def test_proc_capture(jober, tmp_path):
    stdout_fpath = tmp_path / 'stdout.log'

    job = jober.run_job('date', stdout=stdout_fpath)
    job.wait()

    with Path(stdout_fpath).open() as f:
        assert f.read().strip()


#def test_thread_capture(jober, tmp_path):
#    stdout_fpath = tmp_path / 'stdout.log'
#
#    job = jober.run_job(lambda: print('hi'), stdout=stdout_fpath)
#    job.wait()
#
#    with Path(stdout_fpath).open() as f:
#        assert f.read().strip()
