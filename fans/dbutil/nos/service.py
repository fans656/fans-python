import functools

from fans import namer
from fans.path import Path
from fans.bunch import bunch

from .nos import Nos


class Service:
    
    instance = None
    
    @classmethod
    def get_instance(cls, fresh: bool = False):
        if fresh:
            cls.instance = None
        if cls.instance is None:
            cls.instance = cls()
        return cls.instance
    
    def __init__(self):
        self._setup_done = False
        self._name_to_proxy = {}
    
    def setup(self, conf: dict|str):
        if isinstance(conf, str):
            conf = Path(conf).load()
        conf = _normalized_conf(conf)
        
        for store_spec in conf['stores']:
            self._name_to_proxy[store_spec.name] = Proxy(store_spec)
        
        self._setup_done = True
    
    def get(self, name: str):
        self._ensure_setup_done()
        proxy = self._name_to_proxy.get(name)
        if not proxy:
            return None
        return proxy.nos
    
    def info(self):
        self._ensure_setup_done()
        stores = []
        for proxy in self._name_to_proxy.values():
            stores.append(proxy.spec)
        return {'stores': stores}

    def _ensure_setup_done(self):
        if not self._setup_done:
            self.setup([])


class Proxy:
    """For lazy initialization"""
    
    def __init__(self, spec: bunch):
        self.spec = spec
    
    @functools.cached_property
    def nos(self):
        return Nos(**self.spec)


def _normalized_conf(conf: dict|list):
    if isinstance(conf, list):
        conf = {'stores': conf}
    elif isinstance(conf, dict):
        if 'stores' not in conf:
            conf = {'stores': [conf]}
    else:
        raise TypeError(f'invalid conf {conf}')
    
    conf.setdefault('default_store_enabled', True)
    
    conf['stores'] = [bunch(d) for d in conf['stores']]

    namer.ensure_names(conf['stores'], getname=lambda d: d.name or d.path)
    
    if conf['default_store_enabled']:
        if 'default' not in {d.name for d in conf['stores']}:
            conf['stores'].append(bunch({
                'name': 'default',
                'path': ':memory:',
            }))

    return conf
