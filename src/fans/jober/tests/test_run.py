from fans.jober.run import JobRun


def test_status_pubsub(mocker):
    pubsub = mocker.Mock()
    run = JobRun({'pubsub': pubsub})
    run()
    pubsub.publish.assert_called()
    assert run.status == 'done'
