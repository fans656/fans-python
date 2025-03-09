class Test_run:

    def test_run_job(self, mocker, jober):
        """
        Can run a function as job
        """
        func = mocker.Mock()

        jober.run_job(func)
        jober.start()
        jober.run_for_a_while()

        func.assert_called()

    def test_run_job_with_args(self, mocker, jober):
        """
        Can pass args to a function job
        """
        func = mocker.Mock()

        jober.run_job(func, args=[3, 5])
        jober.start()
        jober.run_for_a_while()

        func.assert_called_with(3, 5)

    def test_run_job_with_kwargs(self, mocker, jober):
        """
        Can pass kwargs to a function job
        """
        func = mocker.Mock()

        jober.run_job(func, kwargs={'foo': 3})
        jober.start()
        jober.run_for_a_while()

        func.assert_called_with(foo=3)

    def test_run_job_with_args_and_kwargs(self, mocker, jober):
        """
        Can pass args and kwargs to a function job
        """
        func = mocker.Mock()

        jober.run_job(func, args=[3, 5], kwargs={'foo': 3})
        jober.start()
        jober.run_for_a_while()

        func.assert_called_with(3, 5, foo=3)
    
    def test_get_job_and_run(self, jober, mocker):
        """
        `run_job` returns a `Run` instance, which has run ID and job ID.
        """
        run = jober.run_job(mocker.Mock())

        job = jober.get_job(run.job_id)
        assert job

        assert run.run_id
        assert job.get_run(run.run_id) is run
    
    def test_remove_job(self, jober, mocker):
        """
        can remove existing job
        """
        run = jober.run_job(mocker.Mock())

        jober.start()
        jober.run_for_a_while()

        job = jober.get_job(run.job_id)
        assert job
        
        assert jober.remove_job(job.job_id)
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
