import os

from fans.jober import Jober


class Test_process_job:

    def test_callable_job_has_different_pid(self):
        # TODO: re-write
        return
        func = lambda: None
        job = Jober().make_job(func, mode = 'proc')
        job.start()
        job.join()
        assert job.proc.pid != os.getpid()
