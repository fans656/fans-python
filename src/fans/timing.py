import time

from fans import fmt
from fans.logger import get_logger


default_logger = get_logger(__name__)


class timing:

    def __init__(self, name = None, logger = default_logger):
        self.name = name
        self.logger = logger

    def __enter__(self):
        self.beg = time.time()
        if self.name:
            self.logger.info(f"beg {self.name}...")
        return self

    def __exit__(self, *_):
        self.end = time.time()
        if self.name:
            self.logger.info(f"end {self.name} in {fmt.duration(self.elapsed)}")

    @property
    def elapsed(self):
        self.end = time.time()
        return self.end - self.beg
