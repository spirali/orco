import pytest

from orco.internals.key import make_key, _make_key_helper


def key(x):
    stream = []
    _make_key_helper(x, stream)
    return "".join(stream)


def test_make_key_different_builders():
    assert make_key("abc", 10) != make_key("ab", 10)
    assert make_key("abc", 10) != make_key("abc", 11)


def test_make_key_basics():
    assert key(10) == "10"
    assert key("Hello!") == "'Hello!'"
    assert key(3.14) == "3.14"
    assert key([True, False, 2]) == "[True,False,2,]"

    assert key({"x": 10, "y": 20}) == key({"y": 20, "x": 10})
    assert key({"x": 10, "y": 20}) != key({"y": 10, "x": 20})

    assert key((10, 20)) == key([10, 20])
    assert key([10, 20]) != key([20, 10])


def test_make_key_ignored_keys():
    assert make_key("z", {"x": 10, "_not_here": 20}) == make_key("z", {"x": 10})
    assert make_key("z", {"x": 10, "_not_here": 20}) == make_key("z", {"x": 10, "_no_here": 30})
    assert make_key("z", {"x": 10, "_not_here": 20}) == make_key("z", {"x": 10, "_no_here2": 40})


def test_make_key_invalid():
    class X():
        pass

    with pytest.raises(Exception, match="Invalid item in config"):
        make_key("abc", ["10", X()])

    with pytest.raises(Exception, match="Invalid item in config"):
        make_key("abc", [X()])

    with pytest.raises(Exception, match="Invalid key in config"):
        make_key("abc", {10: 10})
