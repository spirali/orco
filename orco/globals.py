from .builder import Builder
from .runtime import Runtime


_global_builders = {}
_global_runtime = None


def builder(*, name=None, job_setup=None, is_frozen=False):
    def _register(fn):
        b = Builder(fn, name=name, job_setup=job_setup, is_frozen=is_frozen)
        _register_builder(b)
        return b.make_proxy()

    return _register


def _register_builder(b):
    _global_builders[b.name] = b
    if _global_runtime is not None:
        _global_runtime.register_builder(b)


def clear_global_builders():
    _global_builders.clear()


def _get_global_builders():
    return _global_builders.values()


def start_runtime(db_url, *, n_processes=None):
    """
    Create and start a global runtime,

    Runtime manages database with results and starts computations.
    If there were an old runtime, then it is stopped

    For SQLite:

    >>> start_runtime("sqlite:///path/to/dbfile.db")

    For Postgress:

    >>> start_runtime("postgresql://<USERNAME>:<PASSWORD>@<HOSTNAME>/<DATABASE>")
    """

    global _global_runtime
    if _global_runtime is not None:
        _global_runtime.stop()
    _global_runtime = Runtime(db_url, n_processes=n_processes)


def stop_global_runtime():
    global _global_runtime
    if _global_runtime is not None:
        _global_runtime.stop()


def get_global_runtime() -> Runtime:
    global _global_runtime
    if _global_runtime is None:
        raise Exception("No runtime was started")
    return _global_runtime


def has_global_runtime():
    return _global_runtime is not None


def serve(port=8550, debug=False, daemon=False, host="127.0.0.1"):
    return get_global_runtime().serve(port, debug=debug, daemon=daemon, host=host)


def read(job, *, reattach=False):
    return get_global_runtime().read(job, reattach=reattach)


def try_read(job, *, reattach=False):
    return get_global_runtime().read(job, reattach=reattach)


def read_jobs(job):
    return get_global_runtime().read_jobs(job)


def read_many(jobs, *, reattach=False, drop_missing=False):
    return get_global_runtime().read_many(
        jobs, reattach=reattach, drop_missing=drop_missing
    )


def drop(job, *, drop_inputs=False):
    return get_global_runtime().drop(job, drop_inputs=drop_inputs)


def drop_many(jobs, *, drop_inputs=False):
    return get_global_runtime().drop_many(jobs, drop_inputs=drop_inputs)


def archive(job, *, archive_inputs=False):
    return get_global_runtime().archive(job, archive_inputs=archive_inputs)


def archive_many(jobs, *, archive_inputs=False):
    return get_global_runtime().archive_many(jobs, archive_inputs=archive_inputs)


def free(job):
    return get_global_runtime().free(job)


def free_many(jobs):
    return get_global_runtime().free_many(jobs)


def insert(job, value):
    return get_global_runtime().insert(job, value)


def drop_builder(builder_name, *, drop_inputs=False):
    return get_global_runtime().drop_builder(builder_name, drop_inputs=drop_inputs)


def compute(job, *, reattach=False, continue_on_error=False, verbose=True):
    return get_global_runtime().compute(
        job, reattach=reattach, continue_on_error=continue_on_error, verbose=verbose
    )


def compute_many(jobs, *, reattach=False, continue_on_error=False, verbose=True):
    return get_global_runtime().compute_many(
        jobs, reattach=reattach, continue_on_error=continue_on_error, verbose=verbose
    )


def upgrade_builder(builder, upgrade_fn):
    return get_global_runtime().upgrade_builder(builder, upgrade_fn)
