import collections
import functools
import inspect
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


def _generic_kwargs_fn(**kwargs):
    pass


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
        if self.main_fn is not None:
            self.fn_signature = inspect.signature(self.main_fn)
        else:
            self.fn_signature = inspect.signature(_generic_kwargs_fn)

        kwnames = [p.name for p in self.fn_signature.parameters.values() if p.kind == p.VAR_KEYWORD]
        self.kwargs_name = kwnames[0] if kwnames else None
        argnames = [p.name for p in self.fn_signature.parameters.values() if p.kind == p.VAR_POSITIONAL]
        self.args_name = argnames[0] if argnames else None

        self.make_raw_entry = _default_make_raw_entry
        self.job_setup = job_setup
        if update_wrapper:
            functools.update_wrapper(self, fn)

    def _create_config_from_call(self, args, kwargs):
        """
        Return an OrderedDIct of named parameters, unpacking extra kwargs into the dict.
        """
        if self.main_fn is None and args:
            raise Exception("Builders with fn=None only accept keyword arguments")
        ba = self.fn_signature.bind(*args, **kwargs)
        ba.apply_defaults()
        a = ba.arguments
        if self.kwargs_name:
            kwargs = a.pop(self.kwargs_name, {})
            a.update(kwargs)
        return a

    def __call__(self, *args, **kwargs):
        """
        Create an unresolved Entry for this builder.

        Calls `_CONTEXT.on_entry` to register/check dependencies etc.
        """
        config = self._create_config_from_call(args, kwargs)
        entry = Entry(self.name, make_key(config), config, None, None, None)
        if not hasattr(_CONTEXT, "on_entry"):
            return entry
        on_entry = _CONTEXT.on_entry
        if on_entry:
            on_entry(entry)
        return entry

    def run_with_config(self, config):
        "Run the main function with `config`, properly handling `*args` and `**kwargs`."
        assert isinstance(config, dict)
        if self.main_fn is None:
            raise Exception("Fixed builder {!r} can't be run".format(self))

        cfg = collections.OrderedDict(config)  # copy to preserve original
        if self.kwargs_name:
            kwargs = cfg.pop(self.kwargs_name, {})
            cfg.update(kwargs)
        if self.args_name:
            more_args = cfg.pop(self.args_name, ())
        else:
            more_args = ()
        ba = self.fn_signature.bind(**cfg)
        return self.main_fn(*ba.args + more_args, **ba.kwargs)

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
