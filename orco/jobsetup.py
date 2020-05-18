class JobSetup:
    """
    Structure for configuring Job computation.

    Attributes:

    - timeout (int|None): Time limit (in seconds) for computation. If the computation is not finished
               before the limit, an exception is thrown. Default: No time limit.
    - relay (bool): If true, stdout/stderr, redirect output also into the executor's console.
    """

    __slots__ = ("runner_name", "timeout", "setup", "relay", "exclusive")

    def __init__(self, runner_name="local", *, timeout=None, relay=False, setup=None, exclusive=False):
        assert timeout is None or isinstance(timeout, float) or isinstance(timeout, int)
        assert isinstance(relay, bool)
        assert isinstance(exclusive, bool)

        self.runner_name = runner_name
        self.timeout = timeout
        self.setup = setup
        self.relay = relay
        self.exclusive = exclusive

    def __repr__(self):
        return "<JobSetup runner={} timeout={} relay={} exclusive={}>".format(
            self.runner_name, self.timeout, self.relay, self.exclusive
        )
