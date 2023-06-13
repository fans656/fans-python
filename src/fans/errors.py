class Error(Exception):

    def __init__(self, message = '', data = None):
        super().__init__(message, data)
        self.message = message
        self.data = data


class Fail(Error):

    pass


class Stop(Error):

    pass
