from typing import Iterable

from ..ref import Ref


class Task:

    def __init__(self, ref: Ref, inputs: Iterable["Task"], dep_value):
        self.ref = ref
        self.inputs = inputs
        self.dep_value = dep_value
