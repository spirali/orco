import inspect
import pickle

import pytest

from orco.internals.utils import CloudWrapper, format_time


def test_format_time():
    assert format_time(1) == "1.0s"
    assert format_time(1.2) == "1.2s"
    assert format_time(10 / 3) == "3.3s"

    assert format_time(0.1) == "100ms"
    assert format_time(1 / 3) == "333ms"
    assert format_time(0) == "0ms"

    assert format_time(60) == "1.0m"
    assert format_time(1000 / 3) == "5.6m"
    assert format_time(150.3) == "2.5m"

    assert format_time(3600) == "1.0h"
    assert format_time(100000 / 3) == "9.3h"
    assert format_time(1000000000) == "277777.8h"


def test_cloudwrapper():
    fn1 = lambda x: x + 1

    def fn2(x):
        "fiddlesticks"
        return fn1(x) * 2

    cw1 = CloudWrapper(fn1, cache=False)
    assert cw1(3) == 4

    cw2 = CloudWrapper(fn2)
    assert cw2(4) == 10
    assert "fiddlesticks" in cw2.__doc__

    cw1b = pickle.loads(pickle.dumps(cw1))
    assert cw1b(6) == 7
    assert cw1.pickled_fn is None
    assert cw1b.pickled_fn is None

    cw2b = pickle.loads(pickle.dumps(cw2))
    assert cw2b(10) == 22
    assert cw2.pickled_fn is not None
    assert cw2b.pickled_fn == cw2.pickled_fn

    with pytest.raises(AttributeError):
        pickle.dumps(fn1)


def test_cloudwrapper_generator():
    def f(x):
        return x + 1

    cf = CloudWrapper(f)
    assert not cf.is_generator_function()
    assert not inspect.isgenerator(cf(1))

    def g(x):
        yield x + 2
        return x + 3

    cg = CloudWrapper(g)
    assert cg.is_generator_function()
    assert inspect.isgenerator(cg(1))


def test_cloudwrapper_stateful():

    class Fn:
        "foobarbaz"
        def __init__(self, add):
            self.add = add

        def __call__(self, x):
            return x + self.add

    fn = Fn(4)
    cw_c = CloudWrapper(fn, cache=True)
    cw_nc = CloudWrapper(fn, cache=False)
    assert cw_c(10) == 14
    assert cw_nc(10) == 14

    cwb_c = pickle.loads(pickle.dumps(cw_c))
    cwb_nc = pickle.loads(pickle.dumps(cw_nc))
    assert cwb_c(10) == 14
    assert cwb_nc(10) == 14

    fn.add = 1  # Changin state of the callable ...
    cwb_c = pickle.loads(pickle.dumps(cw_c))
    cwb_nc = pickle.loads(pickle.dumps(cw_nc))
    assert cwb_c(10) == 14
    assert cwb_nc(10) == 11  # ... should propagate to here
