
from typing import Iterable
from .ref import Ref
from .collection import Entry

class Task:

    def __init__(self, ref: Ref, inputs: Iterable["Task"], is_computed: bool):
        self.ref = ref
        self.inputs = inputs
        self.is_computed = is_computed