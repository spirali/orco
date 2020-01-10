import argparse
import json
import sys

from .builder import Builder


def _command_serve(runtime, args):
    runtime.serve()


def _command_compute(runtime, args):
    task = Builder(args.builder).task(json.loads(args.config))
    print(runtime.compute(task).value)


def _command_remove(runtime, args):
    builder = runtime.builders.get(args.builder)
    if builder is None:
        raise Exception("Unknown builder '%s'", args.builder)
    builder.remove(json.loads(args.config))


def _parse_args(runtime):
    parser = argparse.ArgumentParser("orco", description="Organized Computing")
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


def run_cli(runtime):
    """
    Start command-line interface over runtime.

    The function always closes runtime on return, even in case of an exception.
    """
    try:
        args = _parse_args(runtime)
        if args.command is None:
            print("No command provided", file=sys.stderr)
        else:
            args.command(runtime, args)
    finally:
        runtime.stop()