
class JobSetup:
    __slots__ = ("runner_name", "timeout", "setup")

    def __init__(self, runner_name="local", timeout=None, setup=None):
        self.runner_name = runner_name
        self.timeout = timeout
        self.setup = setup