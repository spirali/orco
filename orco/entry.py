import collections

from .jobsetup import JobSetup
from orco.consts import MIME_PICKLE, MIME_TEXT
import pickle
import tarfile
import io
import os


EntryMetadata = collections.namedtuple("EntryMetadata", ["created_date", "computation_time", "finished_date", "job_setup"])


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
        value, mime = self._db.get_blob(self._job_id, None)
        if value is None:
            self._value = None
            return None
        value = pickle.loads(value)
        self._value = value
        return value

    def set_job_id(self, job_id, db):
        self._job_id = job_id
        self._db = db

    def make_entry_key(self):
        return EntryKey(self.builder_name, self.key)

    def metadata(self):
        if self._job_id is None:
            raise Exception("Entry is not attached")
        return self._db.read_metadata(self._job_id)

    def get_object(self, name, default=_NO_VALUE):
        value, mime = self.get_blob(name, default)
        if mime != MIME_PICKLE:
            raise Exception("Blob exists, but is not pickled object, but {}".format(mime))
        return pickle.loads(value)

    def get_text(self, name):
        value, mime = self.get_blob(name)
        if mime != MIME_TEXT:
            raise Exception("Blob exists, but is not text, but {}".format(mime))
        return value.decode()

    def get_names(self):
        if self._job_id is None:
            raise Exception("Entry is not attached")
        return self._db.get_blob_names(self._job_id)

    def get_blob(self, name, default=_NO_VALUE):
        if self._job_id is None:
            raise Exception("Entry is not attached")
        value, mime = self._db.get_blob(self._job_id, name)
        if value is None:
            if default is _NO_VALUE:
                raise Exception("Blob '{}' not found".format(name))
            return default
        return value, mime

    def get_blob_as_file(self, name, target=None):
        value, _ = self.get_blob(name)
        if target is None:
            target = name
        with open(target, "wb") as f:
            f.write(value)

    def extract_tar(self, name, target=None):
        value, mime = self.get_blob(name)
        if mime != "application/tar":
            raise Exception("Blob is not tar archive")
        if target is None:
            target = name
        if not os.path.isdir(target):
            os.makedirs(target)
        with tarfile.TarFile(fileobj=io.BytesIO(value)) as tf:
            tf.extractall(target)

    def __repr__(self):
        return "<Entry {}/{}>".format(self.builder_name, self.key)

    def __eq__(self, other):
        if not isinstance(other, Entry):
            return False
        return self.make_entry_key() == other.make_entry_key()

    def __hash__(self):
        return hash((self.make_entry_key()))

EntryKey = collections.namedtuple("EntryKey", ("builder_name", "key"))
