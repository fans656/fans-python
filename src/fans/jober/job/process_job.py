import multiprocessing as mp

from .job import Job


class ProcessJob(Job):

    mode = 'process'
