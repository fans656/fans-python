import pytest
from fans.jober.conftest import parametrized


class Test_run:

    #@parametrized()
    #def test_foo(self, conf, jober):
    #    run = jober.run_job(conf.target, args=('foo',))
    #    run.wait()
    #    assert run.output == 'foo\n'

    def test_run_job(self, mocker, jober):
        """Can run a function as job"""
        func = mocker.Mock()
        jober.run_job(func).wait()
        func.assert_called()

    def test_run_job_with_args_and_kwargs(self, mocker, jober):
        """Can pass args and kwargs to a function job"""
        func = mocker.Mock()
        jober.run_job(func, args=[3, 5], kwargs={'foo': 'bar'}).wait()
        func.assert_called_with(3, 5, foo='bar')

    def test_run_id_and_job_id(self, jober, mocker):
        """Get run ID and job ID"""
        run = jober.run_job(mocker.Mock())
        job = jober.get_job(run.job_id)
        assert job
        assert job.get_run(run.run_id) is run

    def test_remove_job(self, jober, mocker):
        """Can remove existing job"""
        run = jober.run_job(mocker.Mock())
        run.wait()
        assert jober.get_job(run.job_id)
        assert jober.remove_job(run.job_id)
        assert not jober.get_job(run.job_id)

    def test_get_jobs(self, jober, mocker):
        """
        can list jobs
        """
        jober.run_job(mocker.Mock())
        jober.run_job(mocker.Mock())

        assert len(jober.get_jobs()) == 2

    def test_listener(self, jober, mocker):
        """
        can add/remove event listener
        """
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
