from .internals.context import _CONTEXT
from .consts import MIME_PICKLE
from .internals.utils import make_repr

import pickle


def _get_job_context(caller):
    if not hasattr(_CONTEXT, "job_context") or _CONTEXT.job_context is None:
        raise Exception("Function '{}' cannot be called outside of computation part of a builder's function".format(caller))
    return _CONTEXT.job_context


def _validate_name(name):
    if not isinstance(name, str):
        raise Exception("Name has to be a string, not {}".format(type(name)))
    if not name:
        raise Exception("Name has to be a non-empty string")


def attach_object(name, obj):
    _validate_name(name)
    jc = _get_job_context("attach_object")
    jc.db.insert_blob(jc.job_id, name, pickle.dumps(obj), MIME_PICKLE, make_repr(obj))


#def attach_blob(data, data_type):
#    _validate_name(name)
#    jc = _get_job_context("attach_object")
#    jc.db.insert_blob(jc.job_id, name, pickle.dumps(obj), DataType.PICKLE, make_repr(obj))

