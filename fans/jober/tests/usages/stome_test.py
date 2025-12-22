import time

import yaml
import pytest
from fans.path import Path
from fans.bunch import bunch

from fans.jober.job import Job
from fans.jober.jober import Jober
from fans.jober.tests.conftest import parametrized


class Test_api:
    
    def test_start_stop(self):
        jober = Jober()
        jober.start()
        jober.stop()
    
    def test_run_job(self, jober, mocker):
        func = mocker.Mock()

        name = 'paste xxx'
        extra = {'data': ''}
        kwargs = {'foo': 3, 'bar': 5}

        job = jober.run_job(
            func,
            kwargs=kwargs,
            name=name,
            extra=extra,
        )
        job.wait()
        
        # run_job return Job instance
        assert isinstance(job, Job)
        
        # kwargs passed to func
        func.assert_called_with(**kwargs)
