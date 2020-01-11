class Entry:
    """
    A single computation with its configuration and result

    Attributes
    * config - configuration
    * value - resulting value of the computation
    * job_setup - setup for the job
    * created - datetime when entry was created
    * comp_time - time of computation when entry was created, or None if entry was inserted
    """

    __slots__ = ("config", "value", "job_setup", "created", "comp_time")

    def __init__(self, config, value, job_setup, created=None, comp_time=None):
        assert job_setup is None or isinstance(job_setup, dict)
        assert comp_time is None or isinstance(comp_time, float)
        self.config = config
        self.value = value
        self.created = created
        self.job_setup = job_setup
        self.comp_time = comp_time


    @property
    def is_computed(self):
        return bool(self.created)

    def __repr__(self):
        return "<Entry {}>".format(self.config)
