import traceback


class Action:

    def __init__(self, func):
        self.func = func

        self.result = None

    def execute(self):
        try:
            self.result = {'data': self.func()}
        except Exception as exc:
            self.result = {'error': str(exc), 'trace': traceback.format_exc()}


class ClientAction(Action):

    side = 'client'

Action.Client = ClientAction


class ServerAction(Action):

    side = 'server'

Action.Server = ServerAction
