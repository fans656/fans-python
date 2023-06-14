import uuid


class Job:
    """
    Base impl, see separate concrete impl for details.
    """

    def __init__(self, target: any):
        self.target = target
        self.id = uuid.uuid4().hex

    @property
    def source(self):
        return self.target.source

    def schedule(self, spec: dict):
        pass
