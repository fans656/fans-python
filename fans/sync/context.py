import sys
import threading

from fans.logger import get_logger

from .action import Action


logger = get_logger(__name__)


class Context:

    def __init__(self):
        self.timeout = 60
        self.event = threading.Event()
        self.result = None

    @property
    def data(self):
        return self.result.get('data') if self.result else None

    def client(self, func):
        return Action.Client(func)

    def server(self, func):
        return Action.Server(func)

    def communicate(self, action: Action):
        self.send(action.result)
        self.event.wait(timeout=self.timeout)
        self.event.clear()

    def send(self, result: dict):
        pass

    def recv(self, result: dict):
        self.result = result
        self.event.set()


class ClientContext(Context):

    side = 'client'

Context.Client = ClientContext


class ServerContext(Context):

    side = 'server'

Context.Server = ServerContext
