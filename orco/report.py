class Report:
    """
    Report of an event in ORCO. It can be viewed via ORCO browser.

    Attributes:
    * report_type - "info" / "error" / "timeout"
    * executor_id - Id of executor where even comes from
    * message - string representation of message
    * builder_name - name of builder where event occurs (or None if not related)
    * config - config related to the event (or None if not related)
    * timestamp - datetime when event was created
    """

    __slots__ = (
        "report_type",
        "executor_id",
        "message",
        "builder_name",
        "config",
        "timestamp",
    )

    def __init__(
        self,
        report_type,
        executor_id,
        message,
        builder_name=None,
        config=None,
        timestamp=None,
    ):
        self.executor_id = executor_id
        self.timestamp = timestamp
        self.report_type = report_type
        self.message = message
        self.builder_name = builder_name
        self.config = config

    def to_dict(self):
        return {
            "executor": self.executor_id,
            "timestamp": self.timestamp,
            "type": self.report_type,
            "message": self.message,
            "builder": self.builder_name,
            "config": self.config,
        }

    def __repr__(self):
        return "<Report {}: {}>".format(self.report_type, self.message)
