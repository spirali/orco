import collections
from .jobsetup import JobSetup
import pickle


EntryMetadata = collections.namedtuple("EntryMetadata", ["created", "computation_time"])


class _NoValue:
    pass


_NO_VALUE = _NoValue()


class Entry:
    """
    A single computation with its configuration and result

    Attributes
    * config - `OrderedDict` of function parameters (use with `Builder.run_with_config`)
    * value - resulting value of the computation
    * job_setup - setup for the job
    * created - datetime when entry was created
    * comp_time - time of computation when entry was created, or None if entry was inserted
    """

    __slots__ = ("builder_name", "key", "config", "_value", "_job_id", "_db")

    def __init__(self, builder_name, key, config):
        self.builder_name = builder_name
        self.key = key
        self.config = config

        self._job_id = None
        self._db = None
        self._value = _NO_VALUE

    @property
    def value(self):
        value = self._value
        if value is not _NO_VALUE:
            return value
        value = pickle.loads(self._db.get_blob(self._job_id, None))
        self._value = value
        return value

    def set_job_id(self, job_id, db):
        self._job_id = job_id
        self._db = db

    def make_entry_key(self):
        return EntryKey(self.builder_name, self.key)

    def metadata(self):
        if self._job_id is None:
            raise Exception("Entry is detached")
        return self._db.read_metadata(self._job_id)

    def __repr__(self):
        return "<Entry {}/{}>".format(self.builder_name, self.key)

    def __eq__(self, other):
        if not isinstance(other, Entry):
            return False
        return self.make_entry_key() == other.make_entry_key()

    def __hash__(self):
        return hash((self.make_entry_key()))

EntryKey = collections.namedtuple("EntryKey", ("builder_name", "key"))
