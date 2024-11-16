import importlib

from deepmerge import conservative_merger

from fans.path.enhanced import Path

from . import convert
from . import persists


class Store:

    def __init__(self, path):
        self.path = Path(path)

    def subs(self, filter = bool, sorted = sorted):
        if not self.path.is_dir():
            return
        for path in sorted(self.path.iterdir()):
            if filter(path):
                yield Store(path)

    def open(self, *args, **kwargs):
        persist = self._get_persist()
        return persist.open(self.path, *args, **kwargs)

    def load(self, hint = None, default = None, **kwargs):
        hint = normalized_hint(hint)
        persist = self._get_persist(hint)
        try:
            ret = persist.load(self.path, hint, **kwargs)
        except Exception:
            if default is not None or hint and hint.get('silent'):
                ret = default
            else:
                raise
        convert = self._get_convert(hint)
        if convert:
            ret = convert(ret)
        return ret

    def save(self, data, hint = None, **kwargs):
        hint = normalized_hint(hint)
        self.path.ensure_parent()
        return self._get_persist(hint).save(self.path, data, hint, **kwargs)

    def extend(self, data, hint = None, **kwargs):
        hint = normalized_hint(hint)
        self.path.ensure_parent()
        return self._get_persist(hint).extend(self.path, data, hint, **kwargs)

    def update(self, data, hint=None, **kwargs):
        hint = normalized_hint(hint)
        self.path.ensure_parent()
        return self._get_persist(hint).update(self.path, data, hint, **kwargs)

    def readlines(self, convert = None):
        with self.path.open() as f:
            lines = f.readlines()
            return map(convert, lines) if convert else lines

    def ensure_conf(self, default_value: dict = {}) -> dict:
        """
        Ensure path as a config file (YAML/JSON).

        Params:
            default_value - Default conf values.

        Returns:
            the config dict

        Sample usages:

            conf = store.ensure_conf({
                'username': 'admin',
                'password': lambda: uuid.uuid4().hex,
            })
        """
        default_value = eval_lambda_field(default_value)

        persist = self._get_persist()
        if self.path.exists():
            conf = persist.load(self.path)
            conservative_merger.merge(conf, default_value)
        else:
            conf = default_value

        self.save(conf)

        return conf

    def _get_persist(self, hint: dict = None):
        persist = None
        if hint:
            persist = hint.get('persist')
            if isinstance(persist, str):
                match persist:
                    case 'json':
                        persist = json_persist.get_instance()
                    case 'config':
                        persist = conf_persist.get_instance()
                    case _:
                        raise ValueError(f'invalid persist hint: "{persist}"')
        if persist is None:
            suffix = self.path.suffix
            getter = suffix_to_persist.get(suffix)
            if getter:
                persist = getter.get_instance()
        if persist is None:
            raise RuntimeError(f"no suitable persist for {self.path}")
        return persist

    def _get_convert(self, hint):
        if not hint:
            return None
        return hint_to_convert.get(hint.get('usage'))


class PersistGetter:

    def __init__(self, module_name):
        self.module_name = module_name
        self.module = None
        self.persist = None

    def get_instance(self):
        if self.module is None:
            self.module = importlib.import_module(self.module_name)
        if self.persist is None:
            self.persist = self.module.Persist()
        return self.persist


def normalized_hint(hint):
    if not hint:
        return None
    if isinstance(hint, str):
        ret = {}
        keywords = set(hint.split())
        if 'config' in keywords:
            ret['persist'] = 'config'
        if 'json' in keywords:
            ret['persist'] = 'json'
        if 'silent' in keywords:
            ret['silent'] = True
        return ret
    elif isinstance(hint, dict):
        return hint
    else:
        raise ValueError(f"invalid hint {hint}")


def eval_lambda_field(data: dict):
    ret = dict(data)
    for key, value in ret.items():
        if isinstance(value, dict):
            ret[key] = eval_lambda_field(value)
        elif callable(value):
            ret[key] = value()
    return ret


json_persist = PersistGetter('fans.store.persists.json_persist')
yaml_persist = PersistGetter('fans.store.persists.yaml_persist')
conf_persist = PersistGetter('fans.store.persists.conf_persist')
text_persist = PersistGetter('fans.store.persists.text_persist')
jsonlines_persist = PersistGetter('fans.store.persists.jsonlines_persist')
dataframe_persist = PersistGetter('fans.store.persists.dataframe_persist')
pickle_persist = PersistGetter('fans.store.persists.pickle_persist')
suffix_to_persist = {
    '.json': json_persist,
    '.yaml': yaml_persist,
    '.jl': jsonlines_persist,
    '.parq': dataframe_persist,
    '.pickle': pickle_persist,
    '': text_persist,
}
hint_to_convert = {
}
