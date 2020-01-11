from typing import Iterable

from ..task import Task


class Job:

    __slots__ = ("task", "inputs", "dep_value", "job_setup")

    def __init__(self, task: Task, inputs: Iterable["Job"], dep_value, job_setup):
        self.task = task
        self.inputs = inputs
        self.dep_value = dep_value
        self.job_setup = job_setup
