import functools
import pickle

from .entry import Entry
from .internals.context import _CONTEXT
from .internals.key import make_key
from .internals.rawentry import RawEntry


def _default_make_raw_entry(builder_name, key, config, value, job_setup, comp_time):
    value_repr = repr(value)
    if len(value_repr) > 85:
        value_repr = value_repr[:80] + " ..."
    if config is not None:
        config = pickle.dumps(config)
    if job_setup is not None:
        job_setup = pickle.dumps(job_setup)
    return RawEntry(builder_name, key, config, pickle.dumps(value), value_repr, job_setup, comp_time)


class Builder:
    """
    Definition of a single task type, also a factory for an `Entry`.

    If not given, the builder name is the function name.
    Function must be callable or `None` (in that case only manually inserted
    values can be accessed).
    Optionally updates resulting callable object to resemble the wrapped
    function (name, doc, etc.).
    """

    def __init__(self, fn, name: str = None, job_setup=None, update_wrapper=False):
        if not callable(fn) and fn is not None:
            raise TypeError("Fn must be callable or None, {!r} provided".format(fn))
        if name is None:
            if fn is None:
                raise ValueError("Provide at leas one of fn and name")
            name = fn.__name__
        assert isinstance(name, str)
        if not name.isidentifier():
            raise ValueError("{!r} is not a valid name for Builder (needs a valid identifier)".format(name))
        self.name = name
        self.main_fn = fn
        self.make_raw_entry = _default_make_raw_entry
        self.job_setup = job_setup
        if update_wrapper:
            functools.update_wrapper(self, fn)

    def __call__(self, config):
        """
        Create an unresolved Entry for this builder.

        Calls `_CONTEXT.on_entry` to register/check dependencies etc.
        """
        entry = Entry(self.name, make_key(config), config, None, None, None)
        if not hasattr(_CONTEXT, "on_entry"):
            return entry
        on_entry = _CONTEXT.on_entry
        if on_entry:
            on_entry(entry)
        return entry

    def __eq__(self, other):
        if not isinstance(other, Builder):
            return False
        return self.name == other.name

    def __repr__(self):
        return "<{} {!r}>".format(self.__class__.__name__, self.name)

    def _create_job_setup(self, config):
        job_setup = self.job_setup
        if callable(job_setup):
            job_setup = job_setup(config)

        if job_setup is None:
            return {}
        elif isinstance(job_setup, str):
            return {"runner": job_setup}
        elif isinstance(job_setup, dict):
            return job_setup
        else:
            raise TypeError("Invalid object as job_setup: {!r}".format(job_setup))
