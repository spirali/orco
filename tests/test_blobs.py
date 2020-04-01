from orco import builder, attach_object
import pytest


def test_blob_attach(env):

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

