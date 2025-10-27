import pytest

from fans.jober import Jober
from fans.jober.tests.conftest import parametrized


class Test_run:

    @parametrized()
    def test_run_job(self, conf, jober):
        """Can run job and collect output"""
        job = jober.run_job(conf.target, args=('foo',))
        job.wait()
        assert job.output == 'foo\n'

    @parametrized()
    def test_run_job_with_args_and_kwargs(self, conf, jober):
        """Can pass args and kwargs to a function job"""
        run = jober.run_job(conf.target, args=('foo',), kwargs={'count': 2})
        run.wait()
        assert run.output == 'foo\nfoo\n'

    @parametrized()
    def test_run_id_and_job_id(self, conf, jober):
        """Get run ID and job ID"""
        job = jober.run_job(conf.target)
        job = jober.get_job(job.id)
        assert job

    @parametrized()
    def test_remove_job(self, conf, jober):
        """Can remove existing job"""
        job = jober.run_job(conf.target)
        job.wait()
        assert jober.get_job(job.id)
        assert jober.remove_job(job.id)
        assert not jober.get_job(job.id)

    def test_listener(self, jober, mocker):
        """Can add/remove event listener"""
        events = []

        def listener(event):
            events.append(event)

        jober.add_listener(listener)

        jober.run_job(mocker.Mock()).wait()

        assert events
        event_types = {event['type'] for event in events}
        assert 'running' in event_types
        assert 'done' in event_types

        jober.remove_listener(listener)
