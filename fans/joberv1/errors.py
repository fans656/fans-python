class Error(RuntimeError):

    status_code = 500

    def __init__(self, reason, data = None):
        self.reason = reason
        self.data = data


class NotFound(Error):

    status_code = 404


class Conflict(Error):

    status_code = 409
