import yaml


class Persist:

    def load(self, path, hint, **kwargs):
        try:
            with path.open() as f:
                return yaml.safe_load(f, **kwargs)
        except Exception:
            if hint and hint.get('silent'):
                return {}
            else:
                raise
