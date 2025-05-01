from .context import Context


def test_loop():
    ctx = Context()

    with ctx.loop:
        pass
