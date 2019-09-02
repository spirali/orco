from typing import Iterable

from ..task import Task


class Job:

    def __init__(self, task: Task, inputs: Iterable["Job"], dep_value):
        self.task = task
        self.inputs = inputs
        self.dep_value = dep_value
