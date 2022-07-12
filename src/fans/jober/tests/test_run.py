from fans.jober.run import Run


def test_output():
    # TODO
    pass


def test_iter_output():
    # TODO
    pass


def test_iter_output_async():
    # TODO
    pass


def test_status_pubsub(mocker):
    pubsub = mocker.Mock()
    run = Run(on_event = pubsub.publish)
    run()
    pubsub.publish.assert_called()
