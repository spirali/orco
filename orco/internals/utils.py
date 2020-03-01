import pandas as pd


def format_time(seconds):
    if seconds < 0.8:
        return "{:.0f}ms".format(seconds * 1000)
    if seconds < 60:
        return "{:.1f}s".format(seconds)
    if seconds < 3600:
        return "{:.1f}m".format(seconds / 60)
    return "{:.1f}h".format(seconds / 3600)


def unpack_frame(frame, unpack_column="config"):
    new = pd.DataFrame(list(frame[unpack_column]))
    new = pd.concat([frame, new], axis=1)
    new.drop(unpack_column, inplace=True, axis=1)
    return new

import inspect
import cloudpickle

class CloudWrapper:
    """
    Wraps a callable so that cloudpickle is used to pickle it, caching the pickle.
    """
    def __init__(self, fn, pickled_fn=None, cache=True, protocol=cloudpickle.DEFAULT_PROTOCOL):
        if fn is None:
            if pickled_fn is None:
                raise ValueError("Pass at least one of `fn` and `pickled_fn`")
            fn = cloudpickle.loads(pickled_fn)
        assert callable(fn)
        # Forget pickled_fn if it should not be cached
        if pickled_fn is not None and not cache:
            pickled_fn = None

        self.fn = fn
        self.pickled_fn = pickled_fn
        self.cache = cache
        self.protocol = protocol
        self.__doc__ = "CloudWrapper for {!r}. Original doc:\n\n{}".format(self.fn, self.fn.__doc__)

    def __repr__(self):
        return "<{}({!r})>".format(self.__class__.__name__, self.fn)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def _get_pickled_fn(self):
        "Get cloudpickled version of self.fn, optionally caching the result"
        if self.pickled_fn is not None:
            return self.pickled_fn
        print("Pickling {!r}".format(self))
        pfn = cloudpickle.dumps(self.fn, protocol=self.protocol)
        if self.cache:
            self.pickled_fn = pfn
        return pfn

    def __reduce__(self):
        return (self.__class__, (None, self._get_pickled_fn(), self.cache, self.protocol))
