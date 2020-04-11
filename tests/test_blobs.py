import os

import pytest

from orco import builder, attach_object, attach_file, attach_directory, attach_text


def test_blob_attach_object(env):
    @builder()
    def bb(x):
        attach_object("object", x * 100)
        with pytest.raises(Exception, match="already exists"):
            attach_object("object", x * 101)
        attach_object("a-object", x)

    @builder()
    def cc(x):
        b = bb(x)
        with pytest.raises(Exception, match="not attached"):
            b.get_object("xxx")
        with pytest.raises(Exception, match="attach_object"):
            attach_object("object", None)
        yield
        assert ["a-object", "object"] == b.get_names()
        return b.get_object("object") - 1

    runtime = env.test_runtime()
    a = runtime.compute(cc(x=20))
    assert a.value == 1999
    b = runtime.compute(bb(x=20))
    assert b.value is None
    assert b.get_object("object") == 2000

    with pytest.raises(Exception, match="xxx"):
        b.get_object("xxx")


def test_blob_attach_file(env):
    @builder()
    def bb(x):
        with open("test.png", "wb") as f:
            f.write(b"1234")
        attach_file("test.png")
        attach_file("test.png", name="aaa", mime="application/zzz")

    @builder()
    def cc(x):
        b = bb(x)
        yield
        b.get_blob_as_file("test.png")
        with open("test.png", "rb") as f:
            assert f.read() == b"1234"
        b.get_blob_as_file("aaa", "ddd")
        assert not os.path.isfile("aaa")
        with open("ddd", "rb") as f:
            assert f.read() == b"1234"
        return "Ok"

    runtime = env.test_runtime()
    a = runtime.compute(bb(x=20))
    assert a.value is None
    v, m = a.get_blob("test.png")
    assert v == b"1234"
    assert m == "image/png"

    v, m = a.get_blob("aaa")
    assert v == b"1234"
    assert m == "application/zzz"
    a = runtime.compute(cc(x=20))
    assert a.value == "Ok"


def test_blob_attach_text(env):
    @builder()
    def bb(x):
        attach_text("mytext", "Hello world!")

    runtime = env.test_runtime()
    a = runtime.compute(bb(x=20))
    assert a.value is None
    v = a.get_text("mytext")
    assert v == "Hello world!"


def test_blob_attach_directory(env):
    @builder()
    def bb(x):
        os.mkdir("testdir")
        os.mkdir("testdir/subdir")
        with open("testdir/aa.txt", "w") as f:
            f.write("Content 1")

        with open("testdir/bb.txt", "w") as f:
            f.write("Content 2")

        with open("testdir/subdir/cc.txt", "w") as f:
            f.write("Content 3")

        with open("dd.txt", "w") as f:
            f.write("Content 4")

        attach_directory("testdir")
        attach_directory("testdir", name="testdir2")

    @builder()
    def cc(x):
        a = bb(x)
        yield
        assert not os.path.isfile("testdir/testdir/aa.txt")
        a.extract_tar("testdir2")
        assert os.path.isfile("testdir2/aa.txt")
        assert os.path.isfile("testdir2/bb.txt")
        assert os.path.isfile("testdir2/subdir/cc.txt")
        assert not os.path.isfile("aa.txt")
        a.extract_tar("testdir", target=".")
        assert os.path.isfile("aa.txt")
        assert os.path.isfile("bb.txt")
        assert os.path.isfile("subdir/cc.txt")
        return "Ok"

    runtime = env.test_runtime()
    a = runtime.compute(cc(x=20))
    assert a.value == "Ok"
