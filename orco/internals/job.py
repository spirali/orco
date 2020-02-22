from typing import Iterable

from ..entry import Entry


class Job:
    __slots__ = ("entry", "inputs", "deps", "job_setup")

    def __init__(self, entry: Entry, inputs: Iterable["Job"], job_setup):
        self.entry = entry
        self.inputs = inputs
        self.job_setup = job_setup
