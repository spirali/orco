
from typing import Iterable
from .ref import Ref
from .collection import Entry

class Task:

    def __init__(self, ref: Ref, inputs: Iterable["Task"]):
        self.ref = ref
        self.inputs = inputs