class Test_module:

    def test_run_module_job(self, mocker, jober):
        run = jober.run_job('fans.jober.tests.samples.echo', args=['foo'], kwargs={'count': 3})
        run.wait()
        assert run.status == 'done'
        assert run.output == 'foo\nfoo\nfoo\n'
