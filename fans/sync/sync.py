from fans.logger import get_logger

from .server import Server
from .client import Client


logger = get_logger(__name__)


class Sync:

    def __init__(self):
        self.reset()
    
    def reset(self):
        self.server = Server()
        self.client = Client()
    
    def setup_server(self, *args, **kwargs):
        self.server = Server(*args, **kwargs)
    
    def __call__(self, target: str|dict):
        target = normalized_target(target)
        return self.client.sync(target)


def normalized_target(target):
    if isinstance(target, str):
        raise NotImplementedError()
    elif isinstance(target, dict):
        return target
    else:
        raise NotImplementedError()


sync = Sync()
