
class JobSetup:
    """
    relay = redirect output also into the executors console
    """
    __slots__ = ("runner_name", "timeout", "setup", "relay")

    def __init__(self, runner_name="local", timeout=None, relay=False, setup=None):
        self.runner_name = runner_name
        self.timeout = timeout
        self.setup = setup
        self.relay = relay