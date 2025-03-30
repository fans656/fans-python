def sync(ctx: 'fans.sync.Context'):
    @ctx.local
    def _():
        pass
    yield _

    @ctx.remote
    def _():
        pass
    yield i
