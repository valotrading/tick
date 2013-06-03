import sys

from . import cli


def main():
    return cli.start(sys.argv[1:])
