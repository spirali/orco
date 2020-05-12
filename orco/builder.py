import collections
import inspect

from .internals.context import _CONTEXT
from .internals.key import make_key
from .internals.utils import CloudWrapper
from .job import Job
from .jobsetup import JobSetup


def _generic_kwargs_fn(**_kwargs):
    pass


class BuilderProxy:
    def __init__(self, name, has_fn, fn_signature, fn_argspec, fn_name, doc):
        self.name = name
        self.has_fn = has_fn
        self.fn_signature = fn_signature
        self.fn_argspec = fn_argspec
        self.__signature__ = self.fn_signature
        if fn_name:
            self.__name__ = fn_name
        if doc:
            self.__doc__ = "Builder {!r} for {!r}, original docs:\n\n{}\n".format(
                self.name, fn_name, doc
            )

    def __call__(self, *args, **kwargs):
        """
        Create an unresolved Entry for this builder from function arguments.

        Calls `_CONTEXT.on_job` to register/check dependencies etc.
        """
        config = self._create_config_from_args(args, kwargs)
        return self.job_from_config(config)

    def _create_config_from_args(self, args, kwargs):
        """
        Return an OrderedDIct of named parameters, unpacking extra kwargs into the dict.
        """
        if not self.has_fn and args:
            raise Exception("Builders with fn=None only accept keyword arguments")
        ba = self.fn_signature.bind(*args, **kwargs)
        ba.apply_defaults()
        a = ba.arguments
        if self.fn_argspec.varkw:
            kwargs = a.pop(self.fn_argspec.varkw, {})
            a.update(kwargs)
        return a

    def job_from_config(self, config):
        """
        Create an unresolved Entry for this builder from config dict.

        Calls `_CONTEXT.on_job` to register/check dependencies etc.
        """
        job = Job(self.name, make_key(self.name, config), config)
        if not hasattr(_CONTEXT, "on_job"):
            return job
        on_job = _CONTEXT.on_job
        if on_job:
            on_job(job)
        return job

    def __repr__(self):
        return "<BuilderProxy '{}'>".format(self.name)


class Builder:
    """
    Definition of a single task type, also a factory for an `Entry`.

    If not given, the builder name is the function name.
    Function must be callable or `None` (in that case only manually inserted
    values can be accessed).
    Optionally updates resulting callable object to resemble the wrapped
    function (name, doc, etc.).
    """

    def __init__(self, fn, name: str = None, job_setup=None, is_frozen=False):
        if not callable(fn) and fn is not None:
            raise TypeError("Fn must be callable or None, {!r} provided".format(fn))

        if fn is None and not is_frozen:
            raise Exception("When fn is None but builder is not frozen")

        # Cloudwrapper
        if fn is not None and not isinstance(fn, CloudWrapper):
            fn = CloudWrapper(fn)
        self._fn = fn
        if callable(job_setup) and not isinstance(job_setup, CloudWrapper):
            job_setup = CloudWrapper(job_setup)
        self.job_setup = job_setup

        # Name resolution
        if name is None:
            if fn is None:
                raise ValueError("Provide at leas one of fn and name")
            name = fn.__name__
        assert isinstance(name, str)
        if not name.isidentifier():
            raise ValueError(
                "{!r} is not a valid name for Builder (needs a valid identifier)".format(
                    name
                )
            )
        self.name = name
        self.is_frozen = is_frozen

        # Signature inference
        if self.fn is not None:
            self.fn_signature = inspect.signature(self.fn)
            self.fn_argspec = inspect.getfullargspec(self.fn)
        else:
            self.fn_signature = inspect.signature(_generic_kwargs_fn)
            self.fn_argspec = inspect.getfullargspec(_generic_kwargs_fn)

        self.__signature__ = self.fn_signature
        if hasattr(self.fn, "__name__"):
            self.__name__ = self.fn.__name__

    def make_proxy(self):
        name = self.fn.__name__ if self.fn else None
        doc = self.fn.__doc__ if self.fn else None
        return BuilderProxy(
            self.name,
            self._fn is not None,
            self.fn_signature,
            self.fn_argspec,
            name,
            doc,
        )

    @property
    def fn(self):
        if isinstance(self._fn, CloudWrapper):
            return self._fn.fn
        return self._fn

    def run_with_config(self, config, only_deps=False, after_deps=None):
        """
        Run the main function with `config`, properly handling `*args` and `**kwargs`.

        Correctly handles both generator and ordinary main functions, returning the
        final value. With `only_deps=True` only executes the part until `yield`
        (or nothing for ordinary functions).
        Does not set the context etc.
        """
        if self.is_frozen:
            raise Exception("Frozen builder {!r} can't be run".format(self))

        args, kwargs = self._create_args_from_config(config)
        return self.run_with_args(
            args, kwargs, only_deps=only_deps, after_deps=after_deps
        )

    def run_with_args(self, args, kwargs, only_deps=False, after_deps=None):
        """
        Run the main function with `*args` and `**kwargs`.

        Correctly handles both generator and ordinary main functions, returning the
        final value. With `only_deps=True` only executes the part until `yield`
        (or nothing for ordinary functions).

        If given, runs after_deps() after the dependency phase. Does not set the context etc.
        """
        if self.is_frozen:
            raise Exception("Frozen builder {!r} can't be run".format(self))

        if inspect.isgeneratorfunction(self.fn):
            g = self.fn(*args, **kwargs)
            try:
                y = next(g)
            except StopIteration:
                raise Exception("Computation function is a generator but did not yield")
            if y is not None:
                raise Exception("Computation function must yield None")
            if only_deps:
                value = None
            else:
                if after_deps is not None:
                    after_deps()
                try:
                    y = next(g)
                except StopIteration as e:
                    value = e.value
                else:
                    raise Exception("Computation function yielded more than once")
        else:
            if only_deps:
                value = None
            else:
                if after_deps is not None:
                    after_deps()
                value = self.fn(*args, **kwargs)

        if inspect.isgenerator(value):
            raise Exception(
                "Computation function returned a generator while it seemed like an ordinary function"
            )
        return value

    def __eq__(self, other):
        if not isinstance(other, Builder):
            return False
        return self.name == other.name

    def __hash__(self):
        return hash((3726138, self.name))

    def __repr__(self):
        return "<{} {!r}>".format(self.__class__.__name__, self.name)

    def _create_job_setup(self, config):
        job_setup = self.job_setup
        if callable(job_setup):
            job_setup = job_setup(config)

        if job_setup is None:
            return JobSetup("local")
        elif isinstance(job_setup, str):
            return JobSetup(job_setup)
        elif isinstance(job_setup, JobSetup):
            return job_setup
        else:
            raise TypeError("Invalid object as job_setup: {!r}".format(job_setup))

    def _create_args_from_config(self, config):
        """
        Return an (args, kwargs) made from from config.
        
        Unpacks kwargs and args (as named in function signature) in config.
        """
        cfg = collections.OrderedDict(config)  # copy to preserve original
        if self.fn_argspec.varkw:
            kwargs = cfg.pop(self.fn_argspec.varkw, {})
            cfg.update(kwargs)
        if self.fn_argspec.varargs:
            more_args = cfg.pop(self.fn_argspec.varargs, ())
        else:
            more_args = ()
        ba = self.fn_signature.bind(**cfg)
        return (ba.args + more_args, ba.kwargs)
