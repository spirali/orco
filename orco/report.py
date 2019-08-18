class Report:

    __slots__ = ("report_type", "executor_id", "message", "collection_name", "config", "timestamp")

    def __init__(self,
                 report_type,
                 executor_id,
                 message,
                 collection_name=None,
                 config=None,
                 timestamp=None):
        self.executor_id = executor_id
        self.timestamp = timestamp
        self.report_type = report_type
        self.message = message
        self.collection_name = collection_name
        self.config = config

    def to_dict(self):
        return {
            "executor": self.executor_id,
            "timestamp": self.timestamp,
            "type": self.report_type,
            "message": self.message,
            "collection": self.collection_name,
            "config": self.config
        }

    def __repr__(self):
        return "<Report {}: {}>".format(self.report_type, self.message)
