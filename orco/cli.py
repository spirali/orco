import argparse
import sys

import json5

from .builder import Builder
from .cfggen import build_config
from .runtime import Runtime


def _command_serve(runtime, args):
    runtime.serve()


def _command_compute(runtime, args):
    builder = runtime.get_builder(args.builder)
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
    res = runtime.compute_many(tasks)
    for e in res:
        print("{:40s}   {!r}".format(e.key, e.value))


def _command_remove(runtime, args):
    builder = runtime.builders.get(args.builder)
    if builder is None:
        raise Exception("Unknown builder {!r}".format(args.builder))
    builder.remove(json5.loads(args.config))


def _command_drop(runtime, args):
    runtime.drop_builder(args.builder)


def _parse_args():
    parser = argparse.ArgumentParser("orco", description="Organized Computing")
    parser.add_argument("-d", "--db", default=None, type=str)
    sp = parser.add_subparsers(title="command")
    parser.set_defaults(command=None)

    # SERVE
    p = sp.add_parser("serve")
    p.set_defaults(command=_command_serve)

    # COMPUTE
    p = sp.add_parser("compute")
    p.add_argument("builder")
    p.add_argument("config")
    p.set_defaults(command=_command_compute)

    # REMOVE
    p = sp.add_parser("remove")
    p.add_argument("builder")
    p.add_argument("config")
    p.set_defaults(command=_command_remove)

    # DROP
    p = sp.add_parser("drop")
    p.add_argument("builder")
    p.set_defaults(command=_command_drop)

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
                db_path = db_path
            if db_path is None:
                db_path = "sqlite://"
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
