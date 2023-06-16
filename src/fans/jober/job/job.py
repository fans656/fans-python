import uuid
from abc import abstractmethod


class Job:
    """
    Base impl, see separate concrete impl for details.
    """

    mode = None

    def __init__(self, target: any):
        self.target = target
        self.id = uuid.uuid4().hex

        self.init()

    @abstractmethod
    def init(self):
        pass

    @property
    def status(self) -> str:
        """
        Return job status ('ready'|'running'|'done'|'error')
        """
        return None

    @property
    def source(self) -> str:
        return self.target.source


class JobEvent:

    @classmethod
    def run_begin(cls):
        return {
            'type': 'run_begin',
        }

    @classmethod
    def run_done(cls):
        return {
            'type': 'run_done',
        }

    @classmethod
    def run_error(cls):
        return {
            'type': 'run_error',
        }
