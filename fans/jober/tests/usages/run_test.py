import pytest

from fans.jober import Jober
from fans.jober.conftest import parametrized


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
        job = jober.get_job(job.job_id)
        assert job

    @parametrized()
    def test_remove_job(self, conf, jober):
        """Can remove existing job"""
        run = jober.run_job(conf.target)
        run.wait()
        assert jober.get_job(run.job_id)
        assert jober.remove_job(run.job_id)
        assert not jober.get_job(run.job_id)

    def test_listener(self, jober, mocker):
        """Can add/remove event listener"""
        events = []

        def listener(event):
            events.append(event)

        jober.add_listener(listener)

        jober.run_job(mocker.Mock())
        jober.start()
        jober.run_for_a_while()

        assert events
        event_types = {event['type'] for event in events}
        assert 'job_run_begin' in event_types
        assert 'job_run_done' in event_types

        jober.remove_listener(listener)
