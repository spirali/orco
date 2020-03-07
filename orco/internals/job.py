from typing import Iterable

from ..entry import Entry
from collections import namedtuple


class Job:
    """
    Iterface between Executor and Runner,
    represents a unit of computation with already evaluated deps and job_setup
    """

    __slots__ = ("entry", "deps", "job_setup")

    def __init__(self, entry: Entry, deps, job_setup):
        self.entry = entry
        self.deps = deps
        self.job_setup = job_setup


class JobNode:
    __slots__ = ("job", "inputs")

    """
    Interface between Runtime and Executor.
    Represents a graph of computations
    """

    def __init__(self, job: Job, inputs: Iterable["JobNode"]):
        self.job = job
        self.inputs = inputs