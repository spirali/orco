import argparse
import json
import sys

from .builder import Builder
from .runtime import Runtime


def _command_serve(runtime, args):
    runtime.serve()


def _command_compute(runtime, args):
    builder = runtime._get_builder(args.builder)
    task = Builder(builder)(json.loads(args.config))
    print(runtime.compute(task).value)


def _command_remove(runtime, args):
    builder = runtime.builders.get(args.builder)
    if builder is None:
        raise Exception("Unknown builder {!r}".format(args.builder))
    builder.remove(json.loads(args.config))


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

    return parser.parse_args()


def run_cli(runtime=None):
    """
    Start command-line interface over a runtime.

    The function always closes runtime on return, even in case of an exception.

    If not given, a Runtime is created with in-memory db or the db provided with '-d'.
    """
    try:
        args = _parse_args()
        if runtime is None:
            if args.db is None:
                args.db = ":memory:"
            runtime = Runtime(db_path=args.db)
        else:
            if args.db is not None:
                print("Warning: --db ignored (only used with the default runtime)", file=sys.stderr)

        if args.command is None:
            print("No command provided", file=sys.stderr)
        else:
            args.command(runtime, args)
    finally:
        if runtime is not None:
            runtime.stop()
