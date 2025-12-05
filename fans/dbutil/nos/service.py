from fans.path import Path
from fans.bunch import bunch


class Service:
    
    instance = None
    
    @classmethod
    def get_instance(cls):
        if cls.instance is None:
            cls.instance = cls()
        return cls.instance
    
    def __init__(self):
        self._name_to_nos_proxy = {}
    
    def setup(self, conf_path: str):
        conf = Path(conf_path).load()
        conf = _normalized_conf(conf)
        
        for store_spec in conf['stores']:
            store_spec = _normalized_store_spec(store_spec)
            self._name_to_nos_proxy[store_spec.name]


def _normalized_conf(conf: dict|list):
    if isinstance(conf, list):
        conf = {'stores': conf}
    elif isinstance(conf, dict):
        if 'stores' not in conf:
            conf = {'stores': [conf]}
    else:
        raise TypeError(f'invalid conf {conf}')
    
    conf['stores'] = [bunch(d) for d in conf['stores']]

    _ensure_store_names(conf['stores'])

    return conf


def _normalized_store_spec(spec):
    return spec


def _ensure_store_names(specs):
    for spec in specs:
        if 'name' not in spec:
            spec.name = spec.get('path')
    
    existed_names = {d.name for d in specs}
