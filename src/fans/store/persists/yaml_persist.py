import yaml


class Persist:

    def save(self, path, data, hint, **kwargs):
        try:
            with path.open('w') as f:
                return yaml.dump(data, f, **kwargs)
        except:
            return kwargs.get('default')

    def load(self, path, hint, **kwargs):
        try:
            with path.open() as f:
                return yaml.safe_load(f, **kwargs)
        except Exception:
            if hint and hint.get('silent'):
                return {}
            else:
                raise
