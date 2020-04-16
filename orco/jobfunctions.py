import io
import mimetypes
import os
import pickle
import tarfile

from .consts import MIME_PICKLE, MIME_BYTES, MIME_TEXT
from .internals.context import _CONTEXT
from .internals.utils import make_repr


def _get_job_context(caller):
    if not hasattr(_CONTEXT, "job_context") or _CONTEXT.job_context is None:
        raise Exception(
            "Function '{}' cannot be called outside of computation part of a builder's function".format(
                caller
            )
        )
    return _CONTEXT.job_context


def _validate_name(name):
    if not isinstance(name, str):
        raise Exception("Name has to be a string, not {}".format(type(name)))
    if not name:
        raise Exception("Name has to be a non-empty string")
    if name[0] == "!":
        raise Exception("Name cannot start with '!'")


def attach_object(name, obj):
    """
    Attach object to a current job.

    Object is pickled and save under specified name.
    Mime type is set to 'application/python.pickle'
    """
    _validate_name(name)
    jc = _get_job_context("attach_object")
    jc.db.insert_blob(jc.job_id, name, pickle.dumps(obj), MIME_PICKLE, make_repr(obj))


def attach_bytes(name, data, mime=MIME_BYTES, repr=None):
    """
        Attach 'bytes' object to a current job.

        Data are saved as it is under specified name.
    """
    _validate_name(name)
    jc = _get_job_context("attach_bytes")
    jc.db.insert_blob(jc.job_id, name, data, mime, repr)


def attach_text(name, text):
    """
        Attach a text to a current job.

        Text is saved as UTF-8 text with MIME type text/plain".
    """
    _validate_name(name)
    jc = _get_job_context("attach_text")
    jc.db.insert_blob(jc.job_id, name, text.encode(), MIME_TEXT, None)


def attach_directory(path, name=None, repr=None):
    """
        Attach a directory to a current job as tar archive.
    """
    jc = _get_job_context("attach_directory")
    if not os.path.isdir(path):
        raise Exception("Path '{}' is not a directory.".format(path))
    if name is None:
        name = path
    _validate_name(name)
    buf = io.BytesIO()
    with tarfile.TarFile(mode="x", fileobj=buf) as tf:
        for f in os.listdir(path):
            tf.add(os.path.join(path, f), f)
    buf.seek(0)
    jc.db.insert_blob(jc.job_id, name, buf.read(), "application/tar", repr)


def attach_file(filename, name=None, mime=None, repr=None):
    """
        Attach a file to a current job.
    """
    jc = _get_job_context("attach_file")
    with open(filename, "rb") as f:
        data = f.read()
    if name is None:
        name = filename
    _validate_name(name)
    if mime is None:
        mime, _encoding = mimetypes.guess_type(filename)
        if mime is None:
            mime = MIME_BYTES
    jc.db.insert_blob(jc.job_id, name, data, mime, repr)
