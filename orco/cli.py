import argparse
import os
import sys

import json5

from .cfggen import build_config
from .runtime import Runtime
from .globals import has_global_runtime, get_global_runtime


def _command_serve(runtime, args):
    runtime.serve(port=args.port)


def _command_compute(runtime, args):
    tasks = _job_from_args(runtime, args)
    res = runtime.compute_many(tasks)
    for e in res:
        print("{:40s}   {!r}".format(e.key, e.value))


def _job_from_args(runtime, args):
    if not runtime.has_builder(args.builder):
        raise Exception("Unknown builder {!r}".format(args.builder))
    builder = runtime.get_builder(args.builder).make_proxy()
    cfg = json5.loads(args.config)
    cfg = build_config(cfg)
    print(cfg)
    if isinstance(cfg, list):
        tasks = [builder.job_from_config(c) for c in cfg]
    elif isinstance(cfg, dict):
        tasks = [builder.job_from_config(cfg)]
    else:
        raise Exception(
            "Expanded config has type {!r}, list (many tasks) or dict (one task) expected.".format(
                type(cfg)
            )
        )
    return tasks


def _command_drop(runtime, args):
    tasks = _job_from_args(runtime, args)
    runtime.drop_many(tasks)


def _command_archive(runtime, args):
    tasks = _job_from_args(runtime, args)
    runtime.archive_many(tasks)


def _command_free(runtime, args):
    tasks = _job_from_args(runtime, args)
    runtime.free_many(tasks)


def _command_drop_builder(runtime, args):
    runtime.drop_builder(args.builder)


def _parse_args():
    parser = argparse.ArgumentParser("orco", description="Organized Computing")
    parser.add_argument("-d", "--db", default=None, type=str)
    sp = parser.add_subparsers(title="command")
    parser.set_defaults(command=None)

    # SERVE
    p = sp.add_parser("serve")
    p.add_argument("--port", type=int, default=8550)
    p.set_defaults(command=_command_serve)

    # COMPUTE
    p = sp.add_parser("compute")
    p.add_argument("builder")
    p.add_argument("config")
    p.set_defaults(command=_command_compute)

    # DROP
    p = sp.add_parser("drop")
    p.add_argument("builder")
    p.add_argument("config")
    p.set_defaults(command=_command_drop)

    # ARCHIVE
    p = sp.add_parser("archive")
    p.add_argument("builder")
    p.add_argument("config")
    p.set_defaults(command=_command_archive)

    # FREE
    p = sp.add_parser("free")
    p.add_argument("builder")
    p.add_argument("config")
    p.set_defaults(command=_command_free)

    # DROP-BUILDER
    p = sp.add_parser("drop-builder")
    p.add_argument("builder")
    p.set_defaults(command=_command_drop_builder)

    return parser.parse_args()


def run_cli(runtime=None, db_path=None):
    """
    Start command-line interface over a runtime.

    The function always closes runtime on return, even in case of an exception.

    If not given, a Runtime is created with in-memory db or the db provided with '-d'.
    """
    try:
        args = _parse_args()
        if runtime is None:
            if args.db is not None:
                db_path = args.db
            else:
                db_path = os.environ.get("ORCO_DB")
            if db_path is None:
                if has_global_runtime():
                    runtime = get_global_runtime()
                else:
                    raise Exception(
                        "No database is defined, use parameter '--db' or env variable 'ORCO_DB'"
                    )
            else:
                runtime = Runtime(db_path)
        else:
            if args.db is not None:
                print(
                    "Warning: --db ignored (only used with the default runtime)",
                    file=sys.stderr,
                )

        if args.command is None:
            print("No command provided", file=sys.stderr)
        else:
            args.command(runtime, args)
    finally:
        if runtime is not None:
            runtime.stop()
