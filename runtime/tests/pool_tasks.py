"""Tasks at module scope, for tests whose bodies run in task processes: the
processes find them by importing this module. Bodies leave marks in the
directory `KABUDACHI_TEST_MARKERS` names, which a test shares with its task
processes."""

import os
from pathlib import Path

MARKERS = "KABUDACHI_TEST_MARKERS"


def marker(name: str) -> Path:
    return Path(os.environ[MARKERS]) / name


def first_time(name: str) -> bool:
    """True for the first caller of `name` in any process: it makes the mark."""
    try:
        marker(name).open("x").close()
    except FileExistsError:
        return False
    return True
