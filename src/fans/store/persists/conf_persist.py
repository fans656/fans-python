from dynaconf import Dynaconf


class Persist:

    def load(self, path, hint, **kwargs):
        kwargs.setdefault('settings_files', [path])
        return Dynaconf(**kwargs)
