class Persist:

    def load(self, path, hint, **kwargs):
        with path.open() as f:
            return f.read(**kwargs)

    def save(self, path, data, hint, **kwargs):
        # TODO: atomic write
        with path.open('w') as f:
            f.write(data, **kwargs)
